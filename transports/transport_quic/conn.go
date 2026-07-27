package transport_quic

import (
	"net"
	"time"

	quic "github.com/apernet/quic-go"
)

type conn struct {
	stream  *quic.Stream
	session *quic.Conn
	packet  net.PacketConn
	local   net.Addr
	remote  net.Addr
}

func (c *conn) Read(p []byte) (int, error)         { return c.stream.Read(p) }
func (c *conn) Write(p []byte) (int, error)        { return c.stream.Write(p) }
func (c *conn) LocalAddr() net.Addr                { return c.local }
func (c *conn) RemoteAddr() net.Addr               { return c.remote }
func (c *conn) SetDeadline(t time.Time) error      { return c.stream.SetDeadline(t) }
func (c *conn) SetReadDeadline(t time.Time) error  { return c.stream.SetReadDeadline(t) }
func (c *conn) SetWriteDeadline(t time.Time) error { return c.stream.SetWriteDeadline(t) }
func (c *conn) Close() error {
	err := c.stream.Close()
	if closeErr := c.session.CloseWithError(0, "closed"); err == nil {
		err = closeErr
	}
	if closeErr := c.packet.Close(); err == nil {
		err = closeErr
	}
	return err
}
