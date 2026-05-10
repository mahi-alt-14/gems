"""
gem_server.py — Gemini WebAPI FastAPI wrapper with full-jar persistence.
Uses patched gemini_webapi with native rich-cookie-jar support, safe auto_refresh,
QueueingDroppedError detection, and refresh_snlm0e() for 12-hour SNlM0e renewal.
"""

import asyncio
import base64
import json
import mimetypes
import os
import tempfile
import time
import uuid
from pathlib import Path
from typing import Optional

from convex import ConvexClient
from dotenv import load_dotenv
from fastapi import Depends, FastAPI, File, HTTPException, Request, UploadFile
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field

from gemini_webapi import GeminiClient, ChatSession
from gemini_webapi.constants import AccountStatus
from gemini_webapi.exceptions import (
    APIError,
    AuthError,
    GeminiError,
    ModelInvalid,
    QueueingDroppedError,
    TemporarilyBlocked,
    TimeoutError,
    UsageLimitExceeded,
)
from cookies_loader import get_gemini_cookies

load_dotenv()

API_KEY      = os.getenv("GEM_API_KEY", "") or os.getenv("API_KEY", "")
GEM_ID       = os.getenv("GEM_ID", "fcf6f88e78ea")
COOKIES_PATH = Path(__file__).parent / "cookies.json"
UPLOAD_DIR   = Path(__file__).parent / "uploads"
UPLOAD_DIR.mkdir(exist_ok=True)

CONVEX_URL = os.getenv("CONVEX_URL", "").rstrip("/")

_convex_client: Optional[ConvexClient] = None
client: Optional[GeminiClient] = None
_client_lock = asyncio.Lock()


# ── Convex & Local Storage Helpers ──────────────────────────────────────────
def _get_convex() -> Optional[ConvexClient]:
    global _convex_client
    if not CONVEX_URL: return None
    if _convex_client is None: _convex_client = ConvexClient(CONVEX_URL)
    return _convex_client

async def convex_get_cookies() -> Optional[str]:
    cvx = _get_convex()
    if not cvx: return None
    try:
        result = cvx.query("cookies:get")
        return result.get("data") if result and isinstance(result, dict) else None
    except Exception: return None

async def convex_set_cookies(raw_json: str) -> bool:
    cvx = _get_convex()
    if not cvx: return False
    try:
        cvx.mutation("cookies:set", {"data": raw_json})
        return True
    except Exception: return False

def _persist_local(current: dict[str, str]):
    try:
        try: existing = json.loads(COOKIES_PATH.read_text(encoding="utf-8"))
        except Exception: existing =[]

        if isinstance(existing, list):
            by_name = {item["name"]: item for item in existing if isinstance(item, dict) and "name" in item}
            for name, value in current.items():
                if name in by_name: by_name[name]["value"] = value
                else:
                    existing.append({
                        "domain": ".google.com", "hostOnly": False,
                        "httpOnly": False, "name": name, "path": "/",
                        "sameSite": None, "secure": True, "session": False,
                        "storeId": None, "value": value,
                    })
            COOKIES_PATH.write_text(json.dumps(existing, indent=4), encoding="utf-8")
        elif isinstance(existing, dict):
            existing.update(current)
            COOKIES_PATH.write_text(json.dumps(existing, indent=4), encoding="utf-8")
    except Exception: pass

async def persist_cookies():
    if not client or not client._running: return
    try:
        current = {c.name: c.value for c in client.cookies.jar}
        if not current: return
        _persist_local(current)
        if CONVEX_URL: await convex_set_cookies(COOKIES_PATH.read_text(encoding="utf-8"))
    except Exception: pass

async def _load_cookies_json() -> Optional[str]:
    if CONVEX_URL:
        convex_data = await convex_get_cookies()
        if convex_data:
            COOKIES_PATH.write_text(convex_data, encoding="utf-8")
            return convex_data
    if COOKIES_PATH.exists(): return COOKIES_PATH.read_text(encoding="utf-8")
    return None


_HARD_BLOCKS = {
    AccountStatus.ACCOUNT_REJECTED,
    AccountStatus.LOCATION_REJECTED,
    AccountStatus.ACCOUNT_REJECTED_BY_GUARDIAN,
    AccountStatus.GUARDIAN_APPROVAL_REQUIRED,
    AccountStatus.TOS_PENDING,
    AccountStatus.TOS_OUT_OF_DATE,
}

async def _session_warmup(c: GeminiClient) -> bool:
    """Visit Gemini pages with the client's own session to establish legitimacy
    and rotate cookies via Google's Set-Cookie headers."""
    if not c or not c.client or not c._running:
        return False
    try:
        c.client.cookies.update(c._cookies)
        await c.client.get(
            "https://gemini.google.com/app",
            headers={
                "Accept": "text/html,application/xhtml+xml",
                "Sec-Fetch-Dest": "document",
                "Sec-Fetch-Mode": "navigate",
                "Sec-Fetch-Site": "same-origin",
            },
            timeout=20,
        )
        c._cookies.update(c.client.cookies)
        await persist_cookies()
        print("[warmup] Session warmed up successfully.")
        return True
    except Exception as e:
        print(f"[warmup] Failed: {e}")
        return False

# ── Client Init & Auto-Heal ──────────────────────────────────────────────────
def _client_is_alive(c: Optional[GeminiClient]) -> bool:
    return c is not None and c._running and c.account_status not in _HARD_BLOCKS

async def _try_auto_reinit() -> bool:
    global client
    old_client = client
    try:
        raw = await _load_cookies_json()
        if not raw: return False
        psid, psidts, all_cookies = get_gemini_cookies(str(COOKIES_PATH))
        if not psid: return False

        new_client = GeminiClient(secure_1psid=psid, secure_1psidts=psidts or "")
        new_client.cookies = all_cookies

        await new_client.init(
            timeout=60,
            auto_close=False,
            auto_refresh=False,
            verbose=False,
        )

        if new_client.account_status in _HARD_BLOCKS:
            await new_client.close()
            return False

        new_client._cookies.update(new_client.client.cookies)
        new_client.client.cookies.update(new_client._cookies)

        await _session_warmup(new_client)

        async with _client_lock:
            client = new_client
            if old_client and old_client._running:
                asyncio.get_event_loop().call_later(10, lambda c=old_client: asyncio.create_task(c.close()))

        await persist_cookies()
        return True

    except Exception as e:
        print(f"[reinit] failed: {e}")
        return False

async def ensure_client():
    if client is None or not client._running:
        if await _try_auto_reinit(): return client
        raise HTTPException(status_code=503, detail="Client not initialized. Upload cookies.")
    if client.account_status in _HARD_BLOCKS:
        if await _try_auto_reinit(): return client
        raise HTTPException(status_code=401, detail=f"Account blocked ({client.account_status.name}).")
    if client.account_status == AccountStatus.UNAUTHENTICATED:
        try:
            await client.refresh_snlm0e()
            if client.account_status not in _HARD_BLOCKS:
                return client
        except Exception:
            pass
        if await _try_auto_reinit(): return client
        raise HTTPException(status_code=401, detail="Session expired. Upload fresh cookies.")
    return client

async def check_auto_heal(reason: str):
    """If a stream drops to queueing, warmup the session, then try reinit."""
    print(f"[auto-heal] Triggered by: {reason}.")
    if _client_is_alive(client):
        if await _session_warmup(client):
            return
        try:
            await client.refresh_snlm0e()
            print("[auto-heal] SNlM0e refreshed after warmup.")
            return
        except Exception as e:
            print(f"[auto-heal] SNlM0e refresh failed: {e}.")
    await _try_auto_reinit()

def verify_api_key(request: Request):
    if not API_KEY: return
    key = request.headers.get("X-API-Key") or request.query_params.get("api_key") or ""
    auth_header = request.headers.get("Authorization", "")
    if auth_header.startswith("Bearer "): key = key or auth_header[7:]
    if key != API_KEY: raise HTTPException(status_code=401, detail="Invalid API key.")


# ── App ────────────────────────────────────────────────────────────────────────
app = FastAPI(title="Gems API — Convex Managed", version="8.1.0")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_credentials=True, allow_methods=["*"], allow_headers=["*"])

@app.get("/health")
async def health():
    if client is None or not client._running:
        return {"status": "down", "client_initialized": False, "message": "Requires initialization."}
    if client.account_status != AccountStatus.AVAILABLE:
        return {"status": "degraded", "client_initialized": True, "account_status": client.account_status.name}
    return {"status": "healthy", "client_initialized": True, "account_status": client.account_status.name}

@app.post("/cookies/update", dependencies=[Depends(verify_api_key)])
async def update_cookies(file: UploadFile = File(...)):
    content = await file.read()
    try: json.loads(content)
    except json.JSONDecodeError: raise HTTPException(status_code=400, detail="Invalid JSON")

    COOKIES_PATH.write_bytes(content)
    psid, _, _ = get_gemini_cookies(str(COOKIES_PATH))
    if not psid: raise HTTPException(status_code=400, detail="No __Secure-1PSID found.")

    if CONVEX_URL: await convex_set_cookies(content.decode("utf-8"))
    if await _try_auto_reinit():
        return {"status": "cookies_updated", "message": "Full cookies applied, initialized."}
    return {"status": "cookies_updated", "message": "Cookies saved. Init pending."}

@app.post("/reinit", dependencies=[Depends(verify_api_key)])
async def reinit_client():
    """Refresh SNlM0e token without full reinit. Falls back to full reinit on failure."""
    if _client_is_alive(client):
        try:
            await client.refresh_snlm0e()
            await persist_cookies()
            return {"status": "snlm0e_refreshed", "account_status": client.account_status.name}
        except Exception as e:
            print(f"[reinit] SNlM0e refresh failed: {e}. Falling back to full reinit.")

    if await _try_auto_reinit():
        return {"status": "reinitialized", "account_status": client.account_status.name}
    raise HTTPException(status_code=500, detail="Reinit failed.")

SNLM0E_REFRESH_INTERVAL = 14400

@app.post("/warmup", dependencies=[Depends(verify_api_key)])
async def warmup_session():
    """Visit Gemini pages using client's own session to rotate cookies.
    Also refreshes SNlM0e every ~4 hours. Called every 10-15 min by Convex cron."""
    if not _client_is_alive(client):
        return {"status": "skipped", "message": "Client not running."}

    changed = []

    try:
        before = {c.name: c.value for c in client.cookies.jar}

        response = await client.client.get(
            "https://gemini.google.com/app",
            headers={
                "Accept": "text/html,application/xhtml+xml",
                "Sec-Fetch-Dest": "document",
                "Sec-Fetch-Mode": "navigate",
                "Sec-Fetch-Site": "same-origin",
            },
            timeout=20,
        )

        after = {c.name: c.value for c in client.cookies.jar}
        for name, value in after.items():
            if value and (name not in before or before[name] != value):
                changed.append(name)

        client._cookies.update(client.client.cookies)

        if client._init_time and (time.time() - client._init_time >= SNLM0E_REFRESH_INTERVAL):
            try:
                await client.refresh_snlm0e()
                changed.append("SNlM0e")
            except Exception as e:
                print(f"[warmup] SNlM0e refresh failed: {e}")

        if changed:
            await persist_cookies()
            return {"status": "warmed_up", "rotated": changed}
        return {"status": "warmed_up", "rotated": []}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/v1/models")
async def list_models():
    c = await ensure_client()
    return {"object": "list", "data":[{"id": m.model_id, "object": "model", "created": int(time.time()), "owned_by": "google", "permission": [], "root": m.model_id} for m in c.list_models()]}

@app.get("/v1/chats", dependencies=[Depends(verify_api_key)])
async def list_chats():
    c = await ensure_client()
    await c._fetch_recent_chats()
    return {"object": "list", "data":[{"id": ch.cid, "title": getattr(ch, "title", "")} for ch in c.list_chats() if ch.cid]}

@app.get("/v1/chats/{chat_id}", dependencies=[Depends(verify_api_key)])
async def get_chat_history(chat_id: str):
    c = await ensure_client()
    try: history = await c.read_chat(chat_id)
    except Exception as e: raise HTTPException(status_code=404, detail=f"Chat error: {e}")
    messages = ([{"role": turn.role, "content": turn.text or ""} for turn in reversed(history.turns)] if history and history.turns else[])
    return {"id": chat_id, "object": "chat.history", "messages": messages}

@app.delete("/v1/chats/{chat_id}", dependencies=[Depends(verify_api_key)])
async def delete_chat(chat_id: str):
    c = await ensure_client()
    try: await c.delete_chat(chat_id)
    except Exception as e: raise HTTPException(status_code=404, detail=f"Delete error: {e}")
    return {"id": chat_id, "object": "chat.deleted"}

# ── Request/response models & formatting ───────────────────────────────────────
class ImageURL(BaseModel):
    url: str
    detail: Optional[str] = None

class ContentPart(BaseModel):
    type: str
    text: Optional[str] = None
    image_url: Optional[ImageURL] = None

class ChatMessage(BaseModel):
    role: str
    content: str | list[ContentPart]

class ChatCompletionRequest(BaseModel):
    model: str = Field(default="gem")
    messages: list[ChatMessage] = Field(min_length=1)
    temperature: Optional[float] = None
    max_tokens: Optional[int] = None
    stream: bool = False
    gem_id: Optional[str] = Field(default=None)
    chat_id: Optional[str] = Field(default=None)
    metadata: Optional[list] = Field(default=None)

def _completion_id(): return "chatcmpl-" + uuid.uuid4().hex[:29]

def _parse_messages(messages: list[ChatMessage]) -> tuple[str, list[str]]:
    last_user_msg, files = "",[]
    for m in reversed(messages):
        if m.role != "user": continue
        if isinstance(m.content, str):
            last_user_msg = m.content
            break
        if isinstance(m.content, list):
            texts =[]
            for part in m.content:
                if part.type == "text" and part.text: texts.append(part.text)
                elif part.type == "image_url" and part.image_url:
                    fpath = _save_image_data(part.image_url.url)
                    if fpath: files.append(fpath)
            last_user_msg = "\n".join(texts)
            break
    return last_user_msg, files

def _save_image_data(url: str) -> str | None:
    if url.startswith("data:"):
        try:
            header, data = url.split(",", 1)
            ext = mimetypes.guess_extension(header.split(";")[0].split(":")[1]) or ".bin"
            tmp = tempfile.NamedTemporaryFile(suffix=ext, delete=False, dir=UPLOAD_DIR)
            tmp.write(base64.b64decode(data))
            tmp.close()
            return tmp.name
        except Exception: return None
    if url.startswith(("http://", "https://")):
        try:
            import requests as req
            resp = req.get(url, timeout=30)
            if resp.status_code != 200: return None
            ext = mimetypes.guess_extension(resp.headers.get("content-type", "")) or ".bin"
            tmp = tempfile.NamedTemporaryFile(suffix=ext, delete=False, dir=UPLOAD_DIR)
            tmp.write(resp.content)
            tmp.close()
            return tmp.name
        except Exception: return None
    if os.path.isfile(url): return url
    return None

def _encode_chat_id(chat: ChatSession) -> str: return f"{chat.cid}:{chat.rid}:{chat.rcid}" if chat.cid else ""

def _decode_chat_id(raw: str) -> dict:
    parts = raw.split(":")
    return {"cid":  parts[0] if len(parts) > 0 else "", "rid":  parts[1] if len(parts) > 1 else "", "rcid": parts[2] if len(parts) > 2 else ""}

def _extract_metadata(chat: Optional[ChatSession]) -> list:
    if chat is None: return[]
    try:
        if chat.metadata is not None: return list(chat.metadata)
    except Exception: pass
    return[chat.cid or "", chat.rid or "", chat.rcid or ""]

def _new_chat(c: GeminiClient, gem_id: str) -> ChatSession:
    chat = c.start_chat(gem=gem_id, model="gemini-3-flash")
    chat.cid = chat.rid = chat.rcid = ""
    return chat

async def _resume_chat(c: GeminiClient, gem_id: str, metadata: list = None, chat_id: str = None) -> ChatSession:
    cid = chat_id or (metadata[0] if metadata and isinstance(metadata, list) and len(metadata) > 0 else "")
    if not cid: return _new_chat(c, gem_id)
    try:
        latest = await c.fetch_latest_chat_response(cid)
        if latest: return c.start_chat(metadata=list(latest.metadata), cid=cid, rcid=latest.rcid, gem=gem_id, model="gemini-3-flash")
    except Exception: pass
    if metadata: return c.start_chat(metadata=metadata, gem=gem_id, model="gemini-3-flash")
    decoded = _decode_chat_id(cid)
    if decoded["cid"]: return c.start_chat(cid=decoded["cid"], rid=decoded.get("rid", ""), rcid=decoded.get("rcid", ""), gem=gem_id, model="gemini-3-flash")
    return _new_chat(c, gem_id)

def _extract_images(response) -> list[dict]:
    images =[]
    try:
        for img in response.images or[]: images.append({"url": img.url, "title": getattr(img, "title", "") or ""})
    except Exception: pass
    return images

def _build_result(completion_id, created, gem_id, chat, text, candidates, images=None):
    result = {
        "id": completion_id, "object": "chat.completion", "created": created, "model": gem_id,
        "choices":[{"index": 0, "message": {"role": "assistant", "content": text}, "finish_reason": "stop"}],
        "usage": {"prompt_tokens": 0, "completion_tokens": 0, "total_tokens": 0},
        "chat_id": _encode_chat_id(chat), "chat_metadata": _extract_metadata(chat),
    }
    if images: result["choices"][0]["message"]["images"] = images
    for i, cand in enumerate(candidates[1:], 1):
        result["choices"].append({"index": i, "message": {"role": "assistant", "content": cand}, "finish_reason": "stop"})
    return result

async def _send_and_format(chat, prompt, gem_id, files=None):
    try:
        response = await chat.send_message(prompt, files=files or None)
        await persist_cookies()
        text = response.text or ""
        candidates = [c.text for c in (response.candidates or []) if c.text]
        return _build_result(_completion_id(), int(time.time()), gem_id, chat, text, candidates, _extract_images(response))
    except QueueingDroppedError:
        print("[send] Queueing detected. Warming up and retrying...")
        if await _session_warmup(chat.geminiclient):
            try:
                response = await chat.send_message(prompt, files=files or None)
                await persist_cookies()
                text = response.text or ""
                candidates = [c.text for c in (response.candidates or []) if c.text]
                return _build_result(_completion_id(), int(time.time()), gem_id, chat, text, candidates, _extract_images(response))
            except Exception:
                pass
        asyncio.create_task(check_auto_heal("queueing_dropped (send)"))
        raise HTTPException(status_code=502, detail="Google stream dropped (queueing/bot-flag). Session healing, please retry.")
    except HTTPException:
        raise
    except Exception as e:
        asyncio.create_task(check_auto_heal(str(e)))
        raise HTTPException(status_code=502, detail=f"Gemini API Error: {str(e)}")

async def _stream_and_format(chat, prompt, gem_id, files=None):
    completion_id = _completion_id()
    created = int(time.time())

    def _chunk(delta, finish_reason=None, chat=None, images=None):
        meta = _extract_metadata(chat)
        chunk_data = {
            "id": completion_id, "object": "chat.completion.chunk", "created": created, "model": gem_id,
            "choices":[{"index": 0, "delta": delta, "finish_reason": finish_reason}],
        }
        if isinstance(meta, list) and len(meta) > 0 and meta[0]:
            chunk_data["chat_id"] = _encode_chat_id(chat)
            chunk_data["chat_metadata"] = meta
        if images and finish_reason == "stop": chunk_data["images"] = images
        return f"data: {json.dumps(chunk_data, ensure_ascii=False)}\n\n"

    async def event_generator():
        chunk_received = False
        try:
            yield _chunk({"role": "assistant", "content": ""}, None, chat)
            last_response = None
            async for chunk in chat.send_message_stream(prompt, files=files or None):
                chunk_received = True
                last_response = chunk
                if chunk.text_delta: yield _chunk({"content": chunk.text_delta}, None, chat)

            yield _chunk({}, "stop", chat, images=_extract_images(last_response) if last_response else None)
            yield "data: [DONE]\n\n"
            await asyncio.sleep(1.0)
            await persist_cookies()
        except QueueingDroppedError:
            asyncio.create_task(check_auto_heal("queueing_dropped (stream)"))
            yield f"data: {json.dumps({'error': {'message': 'Google stream dropped (queueing/bot-flag). Session healing, please retry.', 'type': 'api_error', 'code': 502}})}\n\ndata: [DONE]\n\n"
        except UsageLimitExceeded as e: yield f"data: {json.dumps({'error': {'message': str(e), 'type': 'rate_limit', 'code': '429'}})}\n\ndata: [DONE]\n\n"
        except TimeoutError as e: yield f"data: {json.dumps({'error': {'message': str(e), 'type': 'timeout', 'code': '504'}})}\n\ndata:[DONE]\n\n"
        except Exception as e:
            asyncio.create_task(check_auto_heal(str(e)))
            yield f"data: {json.dumps({'error': {'message': str(e), 'type': 'api_error', 'code': '502'}})}\n\ndata:[DONE]\n\n"

    return StreamingResponse(event_generator(), media_type="text/event-stream", headers={"Cache-Control": "no-cache", "Connection": "keep-alive", "X-Accel-Buffering": "no"})

@app.post("/v1/chat/completions", dependencies=[Depends(verify_api_key)])
async def chat_completions(req: ChatCompletionRequest):
    last_user_msg, files = _parse_messages(req.messages)
    if not last_user_msg: raise HTTPException(status_code=400, detail="No user message found.")
    c = await ensure_client()
    gem_id = req.gem_id or GEM_ID
    chat = (await _resume_chat(c, gem_id, metadata=req.metadata, chat_id=req.chat_id) if req.metadata or req.chat_id else _new_chat(c, gem_id))
    if req.stream: return await _stream_and_format(chat, last_user_msg, gem_id, files=files or None)
    return await _send_and_format(chat, last_user_msg, gem_id, files=files or None)

@app.on_event("startup")
async def startup():
    global client
    raw = await _load_cookies_json()
    if raw:
        try:
            if await _try_auto_reinit():
                print("[startup] Initialized. Full-jar active. Native auto_refresh + SNlM0e renewal enabled.")
        except Exception as e:
            client = None
            print(f"[startup] Auto-init failed: {e}")

@app.on_event("shutdown")
async def shutdown():
    global client
    if client:
        await persist_cookies()
        await client.close()
        client = None

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=int(os.getenv("GEM_PORT", "8001")))
