# WebSocket relay example

This example contains three programs:

- `relay_server`: Gin HTTP server exposing one isolated path namespace.
- `server`: Telekit server using the WebSocket relay as its ICE path.
- `client`: Telekit client that sends lines and prints the server echo.

The relay has one stable server endpoint ID and authorizes multiple client
IDs. Each client gets an isolated session named
`<session-prefix>:<client-id>`.
The URL path is an outer namespace, defaulting to `/relay/<server-id>`.
Endpoint IDs can be UUIDs or any stable opaque strings. The server creates a
new relay provider for each authenticated client. The client and server must
use the same WebSocket URL path.

The peer programs configure the relay through `peerapi.WithWebSocketRelayServer`;
they do not construct a Pion relay provider or inject relay endpoint IDs into
`client.Options`/`server.Options`. `ICEAgentOptions` remains available for
advanced ICE configuration.

Start the relay server:

```sh
go run ./example/wsrelay/relay_server \
  -listen 0.0.0.0:8080 \
  -relay-address 127.0.0.1 \
  -token change@me \
  -path-prefix /relay \
  -server-public-key '2dc55c63afa1d2ca5d958acf19dafbbf3f77b7752a5204e8ceb881d1cc1b7643' \
  -client-ids client-a
```

Start the Telekit server:

```sh
go run ./example/wsrelay/server \
  -relay-base-url ws://127.0.0.1:8080 \
  -relay-token change@me
```

Start the client in another terminal:

```sh
go run ./example/wsrelay/client \
  -relay-base-url ws://127.0.0.1:8080 \
  -relay-token change@me \
  -client-id client-a
```

The client derives the server endpoint ID from `-server-public-key`. The
server derives the same ID from `-identity-seed`; the relay server derives it
from `-server-public-key`. For another client, use another `-client-id` and
add it to `relay_server -client-ids`. To use a custom path prefix, start the
relay with `-path-prefix /tenant` and pass
`-relay-base-url ws://127.0.0.1:8080/tenant/` to both peers. The server ID is
appended automatically. Use `-path` when specifying the complete path
explicitly, for example `/tenant/<server-id>`.
