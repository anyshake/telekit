# Telekit

Telekit is a peer-to-peer transport library for Go with built-in authentication and encrypted signaling. It uses MQTT, NATS, Centrifugo, or WebSocket for signaling, then exposes the negotiated transport through standard `net.Conn` and `net.Listener` interfaces.

<img src="preview/cloud_shell.png" alt="Telekit preview" style="max-width: 100%; width: 600px; height: auto; display: block; margin: 0 auto;" />

## Use case

Telekit was built for collecting data from sensors behind NAT. The collector acts as the room server, makes outbound connections only, and does not need a public application listener. Sensors authenticate through a signaling service, use Pion ICE for hole punching, and then negotiate QUIC, KCP, SCTP, RakNet, or raw UDP.

Compared with a conventional public TCP/UDP service:

|                      | Public TCP/UDP collector                  | Telekit collector                                  |
| -------------------- | ----------------------------------------- | -------------------------------------------------- |
| Application listener | Publicly reachable address and port       | No public application listener                     |
| Discovery            | Clients connect directly to the collector | Both sides connect outward to signaling            |
| Address disclosure   | Endpoint is visible before authentication | ICE data is released only after PSK authentication |
| Data path            | Public server socket                      | Pion ICE path plus QUIC/KCP/SCTP/RakNet/Raw UDP    |
| Go integration       | `net.Conn`                                | `net.Conn`                                         |

The trade-off is extra signaling and ICE complexity. Direct connectivity is not guaranteed, and strict or symmetric NATs may require TURN.

## Architecture

```text
                            Encrypted signaling
                        ┌────────────────────────┐
                        │ MQTT / NATS /          │
                        │ Centrifugo / WebSocket │
                        └───────────┬────────────┘
                                    │
                            outbound connections
                                    │
        ┌───────────────────────────┴───────────────────────────┐
        │                                                       │
sensor clients behind NAT                          collector server behind NAT
        │                                                       │
        └── Pion ICE >>> QUIC / KCP / SCTP / RakNet / Raw UDP ──┘
```

- A room is one logical sensor network.
- A room has one server and many clients.
- Clients do not talk to each other through Telekit.
- A client needs the room ID, timeout, PSK identity/key, and pinned server public key.
- The QUIC transport uses the Hysteria `quic-go` fork with delivery-rate BBR and pacing, which fits high-latency or lossy ICE paths better than the stock loss-driven controller.
- Signaling adapters carry opaque messages, so the peer layer stays independent of the Broker protocol.

## Security properties

- PSK authentication finishes before any ICE candidate is disclosed.
- Transport capabilities, selection, ICE credentials, and candidates are encrypted with a derived session key.
- Clients pin the server's Ed25519 public key.
- Each connection uses ephemeral X25519 and HKDF to derive its own session key.
- Post-handshake signaling uses AEAD headers, sequence numbers, and replay windows.
- Application frames are encrypted with the session key and the selected transport's own mechanisms.
- Frame sizes, buffers, handshakes, connection counts, and request rates are bounded by configuration.

_The signaling service can still observe routing identifiers, timing, and ciphertext sizes, and can drop, delay, replay, or flood messages. STUN/TURN servers see the network information required by their protocols. An authenticated but compromised client can disclose the Candidate information for that connection._

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

Both peers must use the same base route, and Broker ACLs must authorize it. Each route segment accepts only letters, digits, underscores, and hyphens.

MQTT uses QoS 1 by default and restores subscriptions after reconnecting. Reconnection restores signaling only; applications must redial a closed data transport.

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

// Select explicitly with a transport implementation when needed:
// &client.Options{Transport: transportkcp.New()}
// &client.Options{Transport: transportraknet.New()}
// nil selects the raw UDP transport.
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

Connections expose only the standard `net.Conn` contract: reads, writes, close, addresses, and read/write deadlines. Data-channel message callbacks are an internal transport detail.

## Examples

The [`example`](./example) directory contains independent `net.Conn`, P2P Proxy, P2P DNS, and P2P SSH examples:

```sh
$ go run ./example/netconn/server -room example-netconn -secret change@me
$ go run ./example/netconn/client -room example-netconn -client-id sensor-01 -secret change@me
```

Both programs support `-mqtt` and `-mqtt-base-topic`; the client and server values must match. The shared passphrase and embedded identity are for demonstration only. Production deployments should use a random PSK per device and a private server identity, as described in [`example/README.md`](./example/README.md).

The `p2pssh` example provides SSH shell access, SFTP, and SSH `direct-tcpip` forwarding through Telekit. Unix builds provide PTY-backed shell sessions; Windows builds provide only SFTP and TCP forwarding. See [`example/README.md`](./example/README.md) for commands and options.

## Deployment boundaries

- The Broker requires authentication and narrow topic/subject/channel ACLs.
- The built-in WebSocket Broker denies all connections until `WithAuthorization` is configured.
- Strict NATs may require a reachable TURN relay; a direct path is not guaranteed.
- One server per room is enforced within a process and signaling domain. Multiple instances still need an external lease or leader election.
- `net.Conn` compatibility does not imply `*net.TCPConn`, `syscall.Conn`, or transparent session migration.
- A closed selected transport ends the current connection; application protocols should support reconnectable, idempotent, or resumable transfers.
- This is security-sensitive networking code and should receive an independent review before high-risk deployment.

## License

MIT License © 2026 AnyShake Project
