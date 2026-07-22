# Telekit examples

This directory contains two independent MQTT client/server examples:

- `event` uses callbacks. The server echoes each message, and `ReceiveEventsOnly` avoids duplicating received data in the stream buffer.
- `netconn` uses `net.Listener` and `net.Conn`. The server echoes each stream with `io.Copy`.

```sh
$ go run ./example/event/server -room event-demo -secret change-me
$ go run ./example/event/client -room event-demo -client-id alice -secret change-me
```

Or

```sh
$ go run ./example/netconn/server -room netconn-demo -secret change-me
$ go run ./example/netconn/client -room netconn-demo -client-id alice -secret change-me
```

Use `-mqtt` to select a Broker and `-mqtt-base-topic` to select the topic prefix. Both peers must use the same values:

```sh
$ go run ./example/event/server -mqtt-base-topic sensors/telekit
$ go run ./example/event/client -mqtt-base-topic sensors/telekit
```

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
    fmt.Println("client pinned public key:", hex.EncodeToString(publicKey))
}
```

Pass the outputs to `-identity-seed` and `-server-public-key`, respectively. Keep the seed secret and stable across restarts. Changing it requires reprovisioning every client with the new public key.

The examples hash `-secret` with SHA-256 to obtain a PSK. Production systems should load a separate random key for each device from a secret store instead of sharing a passphrase.

## Resource controls

All four programs expose:

- `-max-frame-bytes`, `-receive-buffer-bytes`, and `-send-buffer-bytes`
- `-max-pending-ice` and `-max-pending-ice-bytes`
- `-mqtt-queue-messages` and `-mqtt-queue-bytes`

Servers also expose connection, pending-handshake, global-buffer, handshake-timeout, and ClientHello rate limits. Event-driven programs add callback worker and queue limits. Run a program with `-h` for the complete list. `-compression` must match on both peers.

Logs use a consistent `[telekit <mode>/<role>]` prefix and do not print PSKs, identity seeds, Broker credentials, Candidates, or application payloads.
