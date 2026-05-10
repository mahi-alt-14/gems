import { query, mutation } from "./_generated/server";
import { v } from "convex/values";

export const get = query({
  args: {},
  handler: async (ctx) => {
    const doc = await ctx.db.query("cookies").order("desc").first();
    if (!doc) return null;
    return { data: doc.data, updatedAt: doc.updatedAt };
  },
});

export const set = mutation({
  args: { data: v.string() },
  handler: async (ctx, args) => {
    const existing = await ctx.db.query("cookies").order("desc").first();
    if (existing) {
      await ctx.db.patch(existing._id, {
        data: args.data,
        updatedAt: Date.now(),
      });
    } else {
      await ctx.db.insert("cookies", {
        data: args.data,
        updatedAt: Date.now(),
      });
    }
  },
});
