import { defineSchema, defineTable } from "convex/server";
import { v } from "convex/values";

export default defineSchema({
  cookies: defineTable({
    data: v.string(),
    updatedAt: v.number(),
  }),
});
