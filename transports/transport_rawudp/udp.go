package transport_rawudp

import (
	"context"
	"errors"
	"io"
	"net"
	"sync"
	"time"

	"github.com/anyshake/telekit/transports"
)

const maxDatagramSize = 64 << 10

// Transport is a raw UDP data transport over the selected ICE candidate pair.
// Each Write is one datagram and each Read returns one datagram. Packet loss,
// duplication, reordering, and truncation follow the underlying UDP path.
type Transport struct {
	// MTU is retained for configuration compatibility and is not used. Raw UDP
	// delegates packet sizing and fragmentation behavior to the underlying path.
	MTU int
}

func (t Transport) Name() string { return "raw_udp" }

func (t Transport) Dial(ctx context.Context, endpoint transports.Endpoint) (net.Conn, error) {
	if endpoint.Conn == nil {
		return nil, errors.New("unsupported transport")
	}
	if err := contextErr(ctx); err != nil {
		return nil, err
	}
	return newConn(endpoint.Conn, endpoint.LocalAddr, endpoint.RemoteAddr)
}

func (t Transport) Accept(ctx context.Context, endpoint transports.Endpoint) (net.Conn, error) {
	if endpoint.Conn == nil {
		return nil, errors.New("unsupported transport")
	}
	if err := contextErr(ctx); err != nil {
		return nil, err
	}
	return newConn(endpoint.Conn, endpoint.LocalAddr, endpoint.RemoteAddr)
}

func contextErr(ctx context.Context) error {
	if ctx == nil {
		return nil
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
		return nil
	}
}

func newConn(base net.Conn, local, remote net.Addr) (net.Conn, error) {
	return &Conn{base: base, local: local, remote: remote}, nil
}

// Conn is the active raw UDP connection returned by Dial and Accept.
type Conn struct {
	base   net.Conn
	local  net.Addr
	remote net.Addr

	writeMu  sync.Mutex
	writeErr error

	closeOnce sync.Once
	closeErr  error
}

func (c *Conn) Read(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	packet := p
	if len(packet) < maxDatagramSize {
		packet = make([]byte, maxDatagramSize)
	}
	n, err := c.base.Read(packet)
	if n > len(p) {
		copy(p, packet[:len(p)])
		return len(p), err
	}
	return n, err
}

func (c *Conn) Write(p []byte) (int, error) {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	if c.writeErr != nil {
		return 0, c.writeErr
	}

	n, err := c.base.Write(p)
	if err != nil {
		c.writeErr = err
		return n, err
	}
	if n != len(p) {
		c.writeErr = io.ErrShortWrite
		return n, c.writeErr
	}
	return n, nil
}

func (c *Conn) LocalAddr() net.Addr  { return c.local }
func (c *Conn) RemoteAddr() net.Addr { return c.remote }

func (c *Conn) SetDeadline(t time.Time) error      { return c.base.SetDeadline(t) }
func (c *Conn) SetReadDeadline(t time.Time) error  { return c.base.SetReadDeadline(t) }
func (c *Conn) SetWriteDeadline(t time.Time) error { return c.base.SetWriteDeadline(t) }

func (c *Conn) Close() error {
	c.closeOnce.Do(func() { c.closeErr = c.base.Close() })
	return c.closeErr
}
