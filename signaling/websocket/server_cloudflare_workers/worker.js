import { extractRoomID, validateRoomID } from "./src/core.js";
import { ROOM_PREFIX, SIGNALING_TOKEN } from "./src/config.js";
import { SignalingRoom } from "./src/durable-object.js";

export { SignalingRoom };

export default {
  async fetch(request, env) {
    const url = new URL(request.url);
    const roomID = extractRoomID(url, ROOM_PREFIX);
    if (roomID === undefined) {
      return new Response("Not Found", { status: 404 });
    }
    if (request.headers.get("upgrade")?.toLowerCase() !== "websocket") {
      return new Response("Expected WebSocket upgrade", { status: 426 });
    }

    const policy = authorize(request, url, roomID, env);
    if (!policy.accepted) {
      return new Response(policy.reason, { status: policy.status });
    }

    const namespace = env.SIGNALING_ROOM;
    if (
      namespace === undefined ||
      typeof namespace.idFromName !== "function" ||
      typeof namespace.get !== "function"
    ) {
      return new Response("SIGNALING_ROOM Durable Object binding is missing", {
        status: 500,
      });
    }

    const durableObjectID = namespace.idFromName(roomID);
    const stub = namespace.get(durableObjectID);
    return stub.fetch(request);
  },
};

function authorize(request, url, roomID, env) {
  if (validateRoomID(roomID) !== undefined) {
    return { accepted: false, status: 400, reason: "Invalid room ID" };
  }

  const token = env.SIGNALING_TOKEN ?? SIGNALING_TOKEN;
  if (url.searchParams.get("token") !== token) {
    return { accepted: false, status: 403, reason: "WebSocket room access denied" };
  }

  const allowedOrigin = env.ALLOWED_ORIGIN;
  const origin = request.headers.get("Origin");
  if (allowedOrigin !== undefined && origin !== allowedOrigin) {
    return { accepted: false, status: 403, reason: "WebSocket origin denied" };
  }

  return { accepted: true };
}
