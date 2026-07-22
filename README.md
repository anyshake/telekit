# Telekit

Telekit is a peer-to-peer transport library for Go with built-in authentication and encrypted signaling. It exchanges encrypted WebRTC/ICE signaling through MQTT, NATS, Centrifugo, or WebSocket, then exposes the resulting DataChannel through event callbacks or standard `net.Conn` / `net.Listener` interfaces.

<img src="preview/cloud_shell.png" alt="Telekit preview" style="max-width: 100%; width: 600px; height: auto; display: block; margin: 0 auto;" />

## Use case

Telekit was built for collecting data from distributed sensors behind NAT. The collector acts as the room server but makes outbound connections only: **it has no public application listener and does not need one**. Sensors authenticate through a signaling service, negotiate a direct WebRTC path when possible, and fall back to TURN when required.

Compared with a conventional public TCP/UDP service:

|                      | Public TCP/UDP collector                  | Telekit collector                                  |
| -------------------- | ----------------------------------------- | -------------------------------------------------- |
| Application listener | Publicly reachable address and port       | No public application listener                     |
| Discovery            | Clients connect directly to the collector | Both sides connect outward to signaling            |
| Address disclosure   | Endpoint is visible before authentication | ICE data is released only after PSK authentication |
| Data path            | Public server socket                      | Direct WebRTC path or encrypted TURN relay         |
| Go integration       | `net.Conn`                                | `net.Conn` or events                               |

The trade-off is additional signaling and ICE complexity. Direct connectivity is not guaranteed, and strict or symmetric NATs may require TURN.

## Architecture

```text
                         Encrypted signaling
                  ┌────────────────────────┐
                  │ MQTT / NATS /          │
                  │ Centrifugo / WebSocket │
                  └───────────┬────────────┘
                              │ outbound connections
              ┌───────────────┴──────────────────────────────┐
              │                                              │
       sensor clients behind NAT               collector server behind NAT
              │                                              │
              └──────── encrypted WebRTC DataChannel ────────┘
```

- A room represents one logical sensor network.
- A room has exactly one server and may have many clients.
- Clients cannot communicate with each other through Telekit.
- A client needs the room ID, timeout, its PSK identity/key, and the pinned server public key to connect.
- Signaling adapters carry opaque messages, keeping the peer layer independent of the Broker protocol.

## Security properties

- PSK authentication completes before any SDP or ICE Candidate is disclosed. **An unauthenticated client cannot obtain the Candidate information required for direct connectivity.**
- SDP, Candidate, and subsequent ICE signaling are encrypted with a derived session key. The Broker cannot read the addresses inside them.
- Clients pin the server's Ed25519 public key, preventing a forged server response from replacing the real server.
- Each connection uses ephemeral X25519 and HKDF to derive an independent session key.
- Post-handshake signaling uses AEAD-authenticated headers, sequence numbers, and replay windows.
- Application frames are encrypted with the session key in addition to WebRTC DTLS protection.
- Frame sizes, buffers, queues, callbacks, handshakes, connection counts, and request rates have configurable bounds.

_The signaling service can still observe routing identifiers, timing, and ciphertext sizes, and can drop, delay, replay, or flood messages. STUN/TURN servers see the network information required by their protocols. An authenticated but compromised client can disclose the Candidate information granted to that connection._

## Signaling adapters

```go
type Adapter interface {
    Connect() error
    Disconnect() error
    Publish(roomID string, typ MessageType, payload []byte) error
    Subscribe(roomID string, typ MessageType, handler Handler) (Subscription, error)
}
```

| Adapter    | Route                         | Default base | Configuration                         |
| ---------- | ----------------------------- | ------------ | ------------------------------------- |
| MQTT       | `{baseTopic}/{room}/{type}`   | `telekit`    | `mqtt.WithBaseTopic(...)`             |
| NATS       | `{baseSubject}.{room}.{type}` | `telekit`    | `nats.NewAdapterWithBaseSubject(...)` |
| Centrifugo | `{baseChannel}:{room}:{type}` | `telekit`    | `centrifugo.WithBaseChannel(...)`     |
| WebSocket  | `{baseURL}/{room}`            | —            | Adapter URL                           |

```go
mqttAdapter, _ := mqtt.NewMQTTAdapter(
    mqttURL,
    mqtt.WithBaseTopic("sensors/telekit"),
)

natsAdapter, _ := nats.NewAdapterWithBaseSubject(
    natsURL,
    "sensors.telekit",
)

centrifugoAdapter, _ := centrifugo.NewAdapter(
    centrifugoURL,
    centrifugo.WithBaseChannel("sensors:telekit"),
)
```

Both peers must use the same base route, and Broker ACLs must authorize that route. Each route segment accepts only letters, digits, underscores, and hyphens, preventing wildcard and hierarchy injection.

MQTT uses QoS 1 by default and restores subscriptions after reconnecting. Reconnection restores signaling only; applications must redial a DataChannel that has already closed.

## `net.Conn` API

A client dials with a room, timeout, device PSK, and pinned server key:

```go
conn, err := client.Dial(
    "sensor-room",
    30*time.Second,
    adapter,
    peer.PreSharedKey{
        ClientID:        "sensor-01",
        Key:             sensorKey,
        ServerPublicKey: pinnedServerPublicKey,
    },
)
if err != nil {
    return err
}
defer conn.Close()

_, err = io.Copy(conn, sensorReader)
```

The server validates device keys and accepts standard `net.Conn` values:

```go
listener, err := server.NewListener(
    "sensor-room",
    adapter,
    peer.StaticKeyring{"sensor-01": sensorKey},
    &server.Options{IdentityKey: serverIdentityPrivateKey},
)
if err != nil {
    return err
}
defer listener.Close()

for {
    conn, err := listener.Accept()
    if err != nil {
        return err
    }
    go collect(conn)
}
```

Connections support reads, writes, close, addresses, and read/write deadlines. Pure event-driven applications can set `ReceiveEventsOnly` to avoid retaining the same data in the stream read buffer.

## Examples

The [`example`](./example) directory contains independent event-driven and `net.Conn` client/server programs:

```sh
# Event-driven
$ go run ./example/event/server -room event-demo -secret change-me
$ go run ./example/event/client -room event-demo -client-id sensor-01 -secret change-me
```

```sh
# net.Conn
$ go run ./example/netconn/server -room netconn-demo -secret change-me
$ go run ./example/netconn/client -room netconn-demo -client-id sensor-01 -secret change-me
```

All four programs support `-mqtt` and `-mqtt-base-topic`; the client and server values must match. The shared passphrase and embedded identity are for demonstration only. Production deployments should use a random PSK per device and provision a private server identity as described in [`example/README.md`](./example/README.md).

## Deployment boundaries

- The Broker requires authentication and narrow topic/subject/channel ACLs.
- The built-in WebSocket Broker denies all connections until `WithAuthorization` is configured.
- Strict NATs may require a reachable TURN relay; a direct path is not guaranteed.
- One-server-per-room is enforced within a process and signaling domain. Multiple instances still require an external lease or leader election.
- `net.Conn` compatibility does not imply `*net.TCPConn`, `syscall.Conn`, or transparent session migration.
- A closed DataChannel ends the current connection; application protocols should support reconnectable, idempotent, or resumable transfers as needed.
- This is security-sensitive networking code and should receive an independent review before high-risk deployment.

## License

MIT License © 2026 AnyShake Project
