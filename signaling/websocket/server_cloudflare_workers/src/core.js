const DEFAULT_MAX_MESSAGE_BYTES = 16 << 20;
const DEFAULT_MAX_CONNECTIONS_PER_ROOM = 256;
const DEFAULT_MAX_CONNECTIONS_PER_IP = 32;
const DEFAULT_MAX_QUEUE_MESSAGES = 64;
const DEFAULT_MAX_QUEUE_BYTES = 16 << 20;

export class SignalingBroker {
  #rooms = new Map();
  #roomReservations = new Map();
  #ipCounts = new Map();
  #options;

  constructor(options = {}) {
    this.#options = {
      authorize: options.authorize ?? (() => false),
      checkOrigin: options.checkOrigin ?? (() => true),
      maxMessageBytes: positive(options.maxMessageBytes, DEFAULT_MAX_MESSAGE_BYTES),
      maxConnectionsPerRoom: positive(options.maxConnectionsPerRoom, DEFAULT_MAX_CONNECTIONS_PER_ROOM),
      maxConnectionsPerIP: positive(options.maxConnectionsPerIP, DEFAULT_MAX_CONNECTIONS_PER_IP),
      maxQueueMessages: positive(options.maxQueueMessages, DEFAULT_MAX_QUEUE_MESSAGES),
      maxQueueBytes: positive(options.maxQueueBytes, DEFAULT_MAX_QUEUE_BYTES),
    };
  }

  async admit(request, roomID) {
    const roomError = validateRoomID(roomID);
    if (roomError !== undefined) {
      return { accepted: false, status: 400, reason: roomError };
    }
    if (!(await this.#options.checkOrigin(request))) {
      return { accepted: false, status: 403, reason: "WebSocket origin denied" };
    }
    if (!(await this.#options.authorize(request, roomID))) {
      return { accepted: false, status: 403, reason: "WebSocket room access denied" };
    }

    const roomConnections =
      (this.#rooms.get(roomID)?.size ?? 0) + (this.#roomReservations.get(roomID) ?? 0);
    const ipConnections = this.#ipCounts.get(request.ip) ?? 0;
    if (
      roomConnections >= this.#options.maxConnectionsPerRoom ||
      ipConnections >= this.#options.maxConnectionsPerIP
    ) {
      return { accepted: false, status: 429, reason: "WebSocket connection limit reached" };
    }

    this.#roomReservations.set(roomID, (this.#roomReservations.get(roomID) ?? 0) + 1);
    this.#ipCounts.set(request.ip, ipConnections + 1);
    let used = false;
    const consumeReservation = () => {
      if (used) {
        return false;
      }
      used = true;
      this.#releaseRoomReservation(roomID);
      return true;
    };

    return {
      accepted: true,
      admission: {
        attach: (socket) => {
          if (!consumeReservation()) {
            throw new Error("WebSocket admission was already consumed");
          }
          try {
            const room = this.#rooms.get(roomID) ?? new Set();
            this.#rooms.set(roomID, room);
            const session = new BrokerSession(this, roomID, request.ip, socket, this.#options);
            room.add(session);
            return session;
          } catch (error) {
            this.#releaseIP(request.ip);
            throw error;
          }
        },
        cancel: () => {
          if (consumeReservation()) {
            this.#releaseIP(request.ip);
          }
        },
      },
    };
  }

  remove(session) {
    const room = this.#rooms.get(session.roomID);
    if (room !== undefined) {
      room.delete(session);
      if (room.size === 0) {
        this.#rooms.delete(session.roomID);
      }
    }
    this.#releaseIP(session.ip);
  }

  broadcast(roomID, data) {
    const room = this.#rooms.get(roomID);
    if (room === undefined) {
      return;
    }
    for (const session of [...room]) {
      session.enqueue(data);
    }
  }

  #releaseIP(ip) {
    const count = this.#ipCounts.get(ip) ?? 0;
    if (count <= 1) {
      this.#ipCounts.delete(ip);
    } else {
      this.#ipCounts.set(ip, count - 1);
    }
  }

  #releaseRoomReservation(roomID) {
    const count = this.#roomReservations.get(roomID) ?? 0;
    if (count <= 1) {
      this.#roomReservations.delete(roomID);
    } else {
      this.#roomReservations.set(roomID, count - 1);
    }
  }
}

export class BrokerSession {
  #closed = false;
  #queue = [];
  #queuedBytes = 0;
  #flushing = false;

  constructor(broker, roomID, ip, socket, options) {
    this.broker = broker;
    this.roomID = roomID;
    this.ip = ip;
    this.socket = socket;
    this.options = options;
  }

  receive(data) {
    if (this.#closed) {
      return;
    }
    const message = copySocketData(data);
    if (socketDataBytes(message) > this.options.maxMessageBytes) {
      this.disconnect(1009, "message too large", true);
      return;
    }
    this.broker.broadcast(this.roomID, message);
  }

  disconnect(code = 1000, reason = "", closeSocket = false) {
    if (this.#closed) {
      return;
    }
    this.#closed = true;
    this.#queue.length = 0;
    this.#queuedBytes = 0;
    this.broker.remove(this);
    if (closeSocket) {
      try {
        this.socket.close(code, reason);
      } catch {
        // The runtime already considers this socket closed.
      }
    }
  }

  enqueue(data) {
    if (this.#closed) {
      return;
    }
    const message = copySocketData(data);
    const bytes = socketDataBytes(message);
    if (
      this.#queue.length >= this.options.maxQueueMessages ||
      this.#queuedBytes + bytes > this.options.maxQueueBytes
    ) {
      this.disconnect(1009, "send queue limit exceeded", true);
      return;
    }
    this.#queue.push(message);
    this.#queuedBytes += bytes;
    this.#scheduleFlush();
  }

  #scheduleFlush() {
    if (this.#flushing) {
      return;
    }
    this.#flushing = true;
    queueMicrotask(() => this.#flush());
  }

  #flush() {
    try {
      while (!this.#closed && this.#queue.length > 0) {
        const message = this.#queue.shift();
        if (message === undefined) {
          break;
        }
        this.#queuedBytes -= socketDataBytes(message);
        this.socket.send(message);
      }
    } catch {
      this.disconnect(1011, "WebSocket send failed", true);
    } finally {
      this.#flushing = false;
      if (!this.#closed && this.#queue.length > 0) {
        this.#scheduleFlush();
      }
    }
  }
}

export function validateRoomID(roomID) {
  if (!/^[A-Za-z0-9_-]{1,128}$/.test(roomID)) {
    return "invalid room ID: use 1-128 letters, digits, underscore, or hyphen";
  }
  return undefined;
}

export function extractRoomID(url, prefix = "/ws") {
  const normalizedPrefix = prefix.replace(/\/$/, "");
  if (!url.pathname.startsWith(`${normalizedPrefix}/`)) {
    return undefined;
  }
  const encodedRoomID = url.pathname.slice(normalizedPrefix.length + 1);
  if (encodedRoomID.length === 0 || encodedRoomID.includes("/")) {
    return undefined;
  }
  try {
    const roomID = decodeURIComponent(encodedRoomID);
    return validateRoomID(roomID) === undefined ? roomID : undefined;
  } catch {
    return undefined;
  }
}

export function requestFromFetch(request, ip = "unknown") {
  return { url: new URL(request.url), headers: request.headers, ip };
}

function positive(value, fallback) {
  return value !== undefined && Number.isFinite(value) && value > 0 ? value : fallback;
}

function copySocketData(data) {
  if (typeof data === "string") {
    return data;
  }
  if (data instanceof ArrayBuffer) {
    return new Uint8Array(data.slice(0));
  }
  return new Uint8Array(data);
}

function socketDataBytes(data) {
  return typeof data === "string" ? new TextEncoder().encode(data).byteLength : data.byteLength;
}
