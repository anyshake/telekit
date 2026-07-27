package transports

import (
	"context"
	"net"
)

type Endpoint struct {
	Conn       net.Conn
	PacketConn net.PacketConn
	LocalAddr  net.Addr
	RemoteAddr net.Addr
}

type ITransport interface {
	Name() string
	Dial(context.Context, Endpoint) (net.Conn, error)
	Accept(context.Context, Endpoint) (net.Conn, error)
}
