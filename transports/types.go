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
	// AuthKey is the per-session key available to transports that need an
	// application-level handshake, such as HTTP/3 fallback authentication.
	AuthKey []byte
}

type ITransport interface {
	Name() string
	Dial(context.Context, Endpoint) (net.Conn, error)
	Accept(context.Context, Endpoint) (net.Conn, error)
}

// PacketModeTransport describes transports whose returned connections
// preserve datagram boundaries.
type PacketModeTransport interface {
	PacketMode() bool
}

// MaxFrameSizeTransport optionally limits the encoded peer frame size. A
// non-positive value means that the transport does not impose an extra limit.
type MaxFrameSizeTransport interface {
	MaxFrameSize() int
}
