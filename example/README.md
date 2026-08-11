# Telekit examples

This directory contains independent MQTT client/server examples for net.Conn, SOCKS5, P2P DNS, and P2P SSH:

- `netconn` uses `net.Listener` and `net.Conn`. The server echoes each stream with `io.Copy`.

```sh
$ go run ./example/netconn/server -room example-netconn -secret change@me
$ go run ./example/netconn/client -room example-netconn -client-id alice -secret change@me
```

Use `-mqtt` to select a Broker and `-mqtt-base-topic` to select the topic prefix. Both peers must use the same values:

```sh
$ go run ./example/netconn/server -mqtt-base-topic sensors/telekit
$ go run ./example/netconn/client -mqtt-base-topic sensors/telekit
```

## WebSocket Relay

`wsrelay` runs both Telekit endpoints through the same authorized WebSocket
relay. See [`wsrelay/README.md`](wsrelay/README.md) for the three-process
example and endpoint-ID configuration.

## P2P Proxy

The proxy example keeps one authenticated Telekit connection per client and multiplexes SOCKS5 TCP streams over it. The client listens on `127.0.0.1:1080`; the server performs the destination TCP dials:

```sh
$ go run ./example/p2proxy/server -room proxy-demo -secret change-me
$ go run ./example/p2proxy/client -room proxy-demo -client-id proxy-alice -secret change-me
$ curl --socks5-hostname 127.0.0.1:1080 https://example.com/
```

The client supports multiple authenticated sessions with `-pool-size`; SOCKS requests are assigned round-robin across the sessions. The server uses an independent upstream DNS resolver (`-dns`) with a short cache and a bounded request worker pool (`-workers`, `-request-queue`). Target TCP connections are not reused because each SOCKS CONNECT stream owns its own half-close lifecycle.

Run another proxy client with a different `-client-id` to use the same server concurrently. The demo server accepts any client ID authenticated by the demo secret; production deployments should use a per-client key provider.

`p2proxy_ws_signaling` is the WebSocket-signaling variant for the Cloudflare
Worker endpoint. See [`p2proxy_ws_signaling/README.md`](p2proxy_ws_signaling/README.md)
for the `-signaling` endpoint format and commands.

## P2P DNS

`p2pdns` forwards encrypted DNS packets over one authenticated P2P connection. Each connection is already bound to one client, so no destination address is carried in the P2P API. The server uses the configured upstream DNS service and the client exposes a local UDP port for ordinary DNS clients. Only the `raw_udp` transport is accepted; DNS packet boundaries are framed by the example itself.

```sh
$ go run ./example/p2pdns/server -room p2pdns-demo -secret change-me -upstream 1.1.1.1:53
$ go run ./example/p2pdns/client -room p2pdns-demo -secret change-me -listen 127.0.0.1:5353
$ dig @127.0.0.1 -p 5353 example.com
```

_Port 53 normally requires elevated privileges. Use an unprivileged `-listen` port while testing._

## P2P SSH

`p2pssh` exposes a local TCP listener that carries SSH, SFTP, and SSH `direct-tcpip` forwarding through an authenticated Telekit connection. The server performs password authentication and generates an SSH host key at `-host-key` when one does not exist.

Start the server and client with matching room, secret, and transport. `-client-id` is optional:

```sh
$ go run ./example/p2pssh/server -room p2pssh-demo -secret change-me -username root -password change-me
$ go run ./example/p2pssh/client -room p2pssh-demo -secret change-me -listen 127.0.0.1:2222
$ ssh -p 2222 root@127.0.0.1
```

SFTP uses the same listener:

```sh
$ sftp -P 2222 root@127.0.0.1
```

SSH TCP forwarding is enabled by default and can be used with `ssh -L` or `ssh -D`. Disable it on the server with `-allow-tcp-forwarding=false`.

The default transport is QUIC. The server advertises QUIC, HTTP/3, KCP, and SCTP. One Telekit connection carries multiple SSH, SFTP, and forwarding sessions through the built-in stream multiplexer.

`p2pssh_ws_signaling` is the WebSocket-signaling variant for the Cloudflare
Worker endpoint. See [`p2pssh_ws_signaling/README.md`](p2pssh_ws_signaling/README.md)
for the `-signaling` endpoint format.

PTY-backed shell sessions are compiled only on non-Windows platforms. Windows server builds reject PTY and shell requests but continue to provide SFTP and SSH TCP forwarding.

## Server identity

The embedded demo identity is public and must not be used in production. Generate an Ed25519 identity once with `crypto/rand`; keep the 32-byte seed on the server and provision only the public key to clients:

```go
package main

import (
    "crypto/ed25519"
    "crypto/rand"
    "encoding/hex"
    "fmt"
    "log"
)

func main() {
    publicKey, privateKey, err := ed25519.GenerateKey(rand.Reader)
    if err != nil {
        log.Fatal(err)
    }
    fmt.Println("server identity seed:", hex.EncodeToString(privateKey.Seed()))
    fmt.Println("server pinned public key:", hex.EncodeToString(publicKey))
}
```

Pass the outputs to `-identity-seed` and `-server-public-key`. Keep the seed secret and stable across restarts. Changing it requires reprovisioning every client with the new public key.

The examples hash `-secret` with SHA-256 to obtain a PSK. Production systems should load a separate random key for each device from a secret store instead of sharing a passphrase.

## Resource controls

The examples expose MQTT queue limits. `netconn` and `p2pssh` also expose generic frame and receive-buffer limits. The `p2proxy` and `p2pssh` client `-transport` value can be `quic`, `http3`, `kcp`, or `sctp`; their servers advertise all four. `p2pdns` accepts only `raw_udp`.

Library users can pass `transports.ITransport` values directly; leaving the server transport unset defaults to reliable QUIC. SCTP/DataChannel tuning stays in the transport implementation.

Servers also expose connection, pending-handshake, global-buffer, handshake-timeout, and ClientHello rate limits. Run a program with `-h` for the complete list.

Logs use a consistent `[telekit <mode>/<role>]` prefix and do not print PSKs, identity seeds, Broker credentials, Candidates, or application payloads.
