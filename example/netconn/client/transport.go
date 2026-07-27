package main

import (
	"fmt"

	"github.com/anyshake/telekit/transports"
	transportkcp "github.com/anyshake/telekit/transports/transport_kcp"
	transportquic "github.com/anyshake/telekit/transports/transport_quic"
	transportraknet "github.com/anyshake/telekit/transports/transport_raknet"
	transportrawudp "github.com/anyshake/telekit/transports/transport_rawudp"
	transportsctp "github.com/anyshake/telekit/transports/transport_sctp"
)

func createTransport(name string) (transports.ITransport, error) {
	switch name {
	case "quic":
		return transportquic.New(), nil
	case "kcp":
		return transportkcp.New(), nil
	case "sctp":
		return transportsctp.New(), nil
	case "raknet":
		return transportraknet.New(), nil
	case "udp", "rawudp":
		return transportrawudp.New(), nil
	default:
		return nil, fmt.Errorf("unsupported transport %q", name)
	}
}
