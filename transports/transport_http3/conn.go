package transport_http3

import (
	"io"
	"net"
	"sync"
	"time"

	quic "github.com/apernet/quic-go"
)

type conn struct {
	reader io.ReadCloser
	writer io.Writer

	session     *quic.Conn
	packet      net.PacketConn
	local       net.Addr
	remote      net.Addr
	closeFn     func()
	closeWriter func()

	closeOnce     sync.Once
	dead          chan struct{}
	deadlineMu    sync.RWMutex
	readDeadline  time.Time
	writeDeadline time.Time
}

func newConn(reader io.ReadCloser, writer io.Writer, session *quic.Conn, packet net.PacketConn, local, remote net.Addr, closeFn, closeWriter func()) *conn {
	return &conn{
		reader:      reader,
		writer:      writer,
		session:     session,
		packet:      packet,
		local:       local,
		remote:      remote,
		closeFn:     closeFn,
		closeWriter: closeWriter,
		dead:        make(chan struct{}),
	}
}

func (c *conn) Read(p []byte) (int, error) {
	if c.expired(true) {
		return 0, net.ErrClosed
	}
	return c.reader.Read(p)
}

func (c *conn) Write(p []byte) (int, error) {
	if c.expired(false) {
		return 0, net.ErrClosed
	}
	return c.writer.Write(p)
}

func (c *conn) LocalAddr() net.Addr  { return c.local }
func (c *conn) RemoteAddr() net.Addr { return c.remote }

func (c *conn) SetDeadline(deadline time.Time) error {
	if err := c.SetReadDeadline(deadline); err != nil {
		return err
	}
	return c.SetWriteDeadline(deadline)
}

func (c *conn) SetReadDeadline(deadline time.Time) error {
	c.deadlineMu.Lock()
	c.readDeadline = deadline
	c.deadlineMu.Unlock()
	return nil
}

func (c *conn) SetWriteDeadline(deadline time.Time) error {
	c.deadlineMu.Lock()
	c.writeDeadline = deadline
	c.deadlineMu.Unlock()
	return nil
}

func (c *conn) expired(read bool) bool {
	c.deadlineMu.RLock()
	deadline := c.writeDeadline
	if read {
		deadline = c.readDeadline
	}
	c.deadlineMu.RUnlock()
	return !deadline.IsZero() && time.Now().After(deadline)
}

func (c *conn) Close() error {
	c.closeOnce.Do(func() {
		close(c.dead)
		if c.reader != nil {
			_ = c.reader.Close()
		}
		if c.closeWriter != nil {
			c.closeWriter()
		}
		if c.session != nil {
			_ = c.session.CloseWithError(0, "closed")
		}
		if c.packet != nil {
			_ = c.packet.Close()
		}
		if c.closeFn != nil {
			c.closeFn()
		}
	})
	return nil
}
