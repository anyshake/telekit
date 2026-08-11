import { extractRoomID, requestFromFetch, SignalingBroker } from "./core.js";
import { ROOM_PREFIX } from "./config.js";

/**
 * One logical Durable Object instance owns one room. The Worker selects the
 * instance with env.SIGNALING_ROOM.idFromName(roomID).
 *
 * WebSocket Hibernation is used here. After a hibernation/restart, the active
 * sockets are restored from state.getWebSockets() before the next message is
 * broadcast.
 */
export class SignalingRoom {
  constructor(state) {
    this.state = state;
    this.roomID = undefined;
    this.sessions = new Map();
    this.broker = new SignalingBroker({
      // The outer Worker performs the public token/origin policy check.
      authorize: () => true,
      checkOrigin: () => true,
    });
  }

  async fetch(request) {
    const url = new URL(request.url);
    const roomID = extractRoomID(url, ROOM_PREFIX);
    if (roomID === undefined) {
      return new Response("Not Found", { status: 404 });
    }
    if (request.headers.get("upgrade")?.toLowerCase() !== "websocket") {
      return new Response("Expected WebSocket upgrade", { status: 426 });
    }

    this.roomID = roomID;
    await this.restoreSessions();

    const ip = request.headers.get("CF-Connecting-IP") ?? "unknown";
    const admission = await this.broker.admit(requestFromFetch(request, ip), roomID);
    if (!admission.accepted) {
      return new Response(admission.reason, { status: admission.status });
    }

    const pair = new WebSocketPair();
    try {
      this.state.acceptWebSocket(pair[1]);
      const session = admission.admission.attach(socketAdapter(pair[1]));
      pair[1].serializeAttachment({ roomID, ip });
      this.sessions.set(pair[1], session);
      return new Response(null, { status: 101, webSocket: pair[0] });
    } catch {
      admission.admission.cancel();
      pair[1].close(1011, "WebSocket setup failed");
      return new Response("WebSocket setup failed", { status: 500 });
    }
  }

  async webSocketMessage(socket, message) {
    await this.restoreSessions();
    this.sessions.get(socket)?.receive(message);
  }

  async webSocketClose(socket) {
    await this.restoreSessions();
    this.sessions.get(socket)?.disconnect();
    this.sessions.delete(socket);
  }

  async webSocketError(socket) {
    await this.restoreSessions();
    this.sessions.get(socket)?.disconnect(1011, "WebSocket error");
    this.sessions.delete(socket);
  }

  async restoreSessions() {
    const sockets = this.state.getWebSockets();
    if (sockets.length === 0) {
      return;
    }

    if (this.roomID === undefined) {
      const attachment = sockets[0].deserializeAttachment();
      this.roomID = attachment?.roomID;
    }
    if (this.roomID === undefined) {
      return;
    }

    for (const socket of sockets) {
      if (this.sessions.has(socket)) {
        continue;
      }
      const attachment = socket.deserializeAttachment();
      const ip = attachment?.ip ?? "unknown";
      const admission = await this.broker.admit(
        requestFromFetch(
          new Request(`https://durable-object.invalid${ROOM_PREFIX}/${this.roomID}`),
          ip,
        ),
        this.roomID,
      );
      if (!admission.accepted) {
        socket.close(1013, "Room connection limit reached");
        continue;
      }
      try {
        const session = admission.admission.attach(socketAdapter(socket));
        this.sessions.set(socket, session);
      } catch {
        admission.admission.cancel();
        socket.close(1011, "WebSocket restore failed");
      }
    }
  }
}

function socketAdapter(socket) {
  return {
    send: (data) => socket.send(data),
    close: (code, reason) => socket.close(code, reason),
  };
}
