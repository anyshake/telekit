package main

import (
	"github.com/anyshake/telekit/transports"
	transportkcp "github.com/anyshake/telekit/transports/transport_kcp"
	transportquic "github.com/anyshake/telekit/transports/transport_quic"
	transportrawudp "github.com/anyshake/telekit/transports/transport_rawudp"
	transportsctp "github.com/anyshake/telekit/transports/transport_sctp"
)

func serverTransports() []transports.ITransport {
	return []transports.ITransport{
		transportquic.New(),
		transportkcp.New(),
		transportsctp.New(),
		transportrawudp.New(),
	}
}
