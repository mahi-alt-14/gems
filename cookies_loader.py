import json
import time
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from pathlib import Path
from typing import Optional


def _parse_expiry(value) -> Optional[int]:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return int(value)
    if isinstance(value, str):
        raw = value.strip()
        if not raw:
            return None
        try:
            return int(float(raw))
        except ValueError:
            pass
        try:
            dt = datetime.fromisoformat(raw.replace("Z", "+00:00"))
            return int(dt.timestamp())
        except ValueError:
            pass
        try:
            dt = parsedate_to_datetime(raw)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return int(dt.timestamp())
        except Exception:
            return None
    return None


def load_cookies(path: str | Path) -> tuple[dict[str, str], dict[str, dict]]:
    data = json.loads(Path(path).read_text(encoding="utf-8"))
    cookies: dict[str, str] = {}
    meta: dict[str, dict] = {}

    def _upsert(name, value, expires_raw=None):
        if not isinstance(name, str) or not name:
            return
        if not isinstance(value, str) or not value:
            return
        cookies[name] = value
        exp = _parse_expiry(expires_raw)
        meta[name] = {
            "expires_epoch": exp,
        }

    def _handle_obj(item):
        name = item.get("name")
        value = item.get("value")
        expires_raw = (
            item.get("expirationDate")
            or item.get("expires")
            or item.get("expiry")
            or item.get("expiresDate")
        )
        _upsert(name, value, expires_raw=expires_raw)

    if isinstance(data, dict) and all(
        isinstance(k, str) and isinstance(v, str) for k, v in data.items()
    ):
        for k, v in data.items():
            _upsert(k, v)
        return cookies, meta

    if isinstance(data, dict) and isinstance(data.get("cookies"), dict):
        inner = data["cookies"]
        if all(isinstance(v, str) for v in inner.values()):
            for k, v in inner.items():
                _upsert(k, v)
            return cookies, meta

    if isinstance(data, dict) and isinstance(data.get("cookies"), list):
        for item in data["cookies"]:
            if isinstance(item, dict):
                _handle_obj(item)
        if cookies:
            return cookies, meta

    if isinstance(data, list):
        for item in data:
            if isinstance(item, dict):
                _handle_obj(item)
        if cookies:
            return cookies, meta

    raise ValueError(f"Unsupported cookies format in {path}")


def get_gemini_cookies(path: str | Path) -> tuple[str, str, dict[str, str]]:
    cookies, _ = load_cookies(path)
    psid = cookies.get("__Secure-1PSID", "")
    psidts = cookies.get("__Secure-1PSIDTS", "")
    return psid, psidts, cookies
