import { cronJobs } from "convex/server";
import { internal } from "./_generated/api";
import { internalAction } from "./_generated/server";

const URL = "https://gems-axpa.onrender.com"; // Update this!
const API_KEY = "suvawillshineagain";                     // Update this!

// Hits the new stateless warmup endpoint to prevent 45-min 1PSIDTS drops
export const pingWarmup = internalAction({
  args: {},
  handler: async () => {
    try {
      await fetch(`${URL}/warmup`, { method: "POST", headers: { "X-API-Key": API_KEY } });
    } catch (e) {}
  },
});

// Rebuilds the client from scratch every 4 hours to prevent 12-hour SNlM0e drops
export const pingReinit = internalAction({
  args: {},
  handler: async () => {
    try {
      await fetch(`${URL}/reinit`, { method: "POST", headers: { "X-API-Key": API_KEY } });
    } catch (e) {}
  },
});

const crons = cronJobs();

// Schedule both loops via Convex Serverless infrastructure
crons.interval("Warmup Session", { minutes: 15 }, internal.crons.pingWarmup, {});
crons.interval("Rebuild Client", { hours: 4 }, internal.crons.pingReinit, {});

export default crons;
