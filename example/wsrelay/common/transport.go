package common

import (
	"fmt"

	"github.com/anyshake/telekit/transports"
	transporthttp3 "github.com/anyshake/telekit/transports/transport_http3"
	transportkcp "github.com/anyshake/telekit/transports/transport_kcp"
	transportquic "github.com/anyshake/telekit/transports/transport_quic"
	transportrawudp "github.com/anyshake/telekit/transports/transport_rawudp"
	transportsctp "github.com/anyshake/telekit/transports/transport_sctp"
)

func CreateTransport(name string) (transports.ITransport, error) {
	switch name {
	case "quic":
		return transportquic.New(), nil
	case "http3":
		return transporthttp3.New(), nil
	case "kcp":
		return transportkcp.New(), nil
	case "sctp":
		return transportsctp.New(), nil
	case "udp", "rawudp":
		return transportrawudp.New(), nil
	default:
		return nil, fmt.Errorf("unsupported transport %q", name)
	}
}

func ServerTransports() []transports.ITransport {
	return []transports.ITransport{
		transportquic.New(),
		transporthttp3.New(),
		transportkcp.New(),
		transportsctp.New(),
		transportrawudp.New(),
	}
}
