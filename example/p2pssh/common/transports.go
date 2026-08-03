package common

import (
	"fmt"

	"github.com/anyshake/telekit/transports"
	transportkcp "github.com/anyshake/telekit/transports/transport_kcp"
	transportquic "github.com/anyshake/telekit/transports/transport_quic"
	transportsctp "github.com/anyshake/telekit/transports/transport_sctp"
)

func NewTransport(name string) (transports.ITransport, error) {
	switch name {
	case "quic":
		return transportquic.New(), nil
	case "kcp":
		return transportkcp.New(), nil
	case "sctp":
		return transportsctp.New(), nil
	default:
		return nil, fmt.Errorf("unsupported transport %q", name)
	}
}

func ServerTransports() []transports.ITransport {
	return []transports.ITransport{
		transportquic.New(),
		transportkcp.New(),
		transportsctp.New(),
	}
}
