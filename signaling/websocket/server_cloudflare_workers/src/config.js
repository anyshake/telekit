// Keep the public route identical in the Worker and Durable Object. The Go
// WebSocket adapter appends the room ID after this prefix.
export const ROOM_PREFIX = "/ws";

// Development fallback. In production, prefer the SIGNALING_TOKEN secret;
// worker.js gives the environment binding precedence over this value.
export const SIGNALING_TOKEN = "passme";
