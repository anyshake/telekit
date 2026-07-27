package transports

import (
	"errors"
	"net"
	"strings"
	"time"
)

// PacketConn adapts the packet-preserving ICE connection to net.PacketConn.
// ICE has already selected one remote candidate pair, so all packets have the
// same remote address.
type PacketConn struct {
	conn   net.Conn
	local  net.Addr
	remote net.Addr
}

func NewPacketConn(conn net.Conn, local, remote net.Addr) *PacketConn {
	return &PacketConn{conn: conn, local: local, remote: remote}
}

func (c *PacketConn) ReadFrom(p []byte) (int, net.Addr, error) {
	n, err := c.conn.Read(p)
	return n, c.remote, normalizeCloseError(err)
}

func (c *PacketConn) WriteTo(p []byte, addr net.Addr) (int, error) {
	if addr != nil && c.remote != nil && addr.String() != c.remote.String() {
		return 0, errors.New("packet destination does not match ICE peer")
	}
	n, err := c.conn.Write(p)
	return n, normalizeCloseError(err)
}

func (c *PacketConn) Close() error        { return normalizeCloseError(c.conn.Close()) }
func (c *PacketConn) LocalAddr() net.Addr { return c.local }
func (c *PacketConn) SetDeadline(t time.Time) error {
	return normalizeCloseError(c.conn.SetDeadline(t))
}
func (c *PacketConn) SetReadDeadline(t time.Time) error {
	return normalizeCloseError(c.conn.SetReadDeadline(t))
}
func (c *PacketConn) SetWriteDeadline(t time.Time) error {
	return normalizeCloseError(c.conn.SetWriteDeadline(t))
}

// normalizedConn converts Pion ICE's close error into the standard net.Conn
// error. Protocol implementations use errors.Is(err, net.ErrClosed) to stop
// their read loops, while Pion ICE currently returns a descriptive error.
type normalizedConn struct{ net.Conn }

func (c normalizedConn) Read(p []byte) (int, error) {
	n, err := c.Conn.Read(p)
	return n, normalizeCloseError(err)
}

func (c normalizedConn) Write(p []byte) (int, error) {
	n, err := c.Conn.Write(p)
	return n, normalizeCloseError(err)
}

func (c normalizedConn) Close() error { return normalizeCloseError(c.Conn.Close()) }

func (c normalizedConn) SetDeadline(t time.Time) error {
	return normalizeCloseError(c.Conn.SetDeadline(t))
}

func (c normalizedConn) SetReadDeadline(t time.Time) error {
	return normalizeCloseError(c.Conn.SetReadDeadline(t))
}

func (c normalizedConn) SetWriteDeadline(t time.Time) error {
	return normalizeCloseError(c.Conn.SetWriteDeadline(t))
}

func normalizeCloseError(err error) error {
	if err == nil || errors.Is(err, net.ErrClosed) {
		return err
	}
	if strings.Contains(err.Error(), "agent is closed") {
		return net.ErrClosed
	}
	return err
}
