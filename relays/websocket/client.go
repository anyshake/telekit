package websocket

import (
	"io"
	"net"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

type packetConn struct {
	conn  *websocket.Conn
	local *net.UDPAddr

	readMu    sync.Mutex
	writeMu   sync.Mutex
	closeOnce sync.Once
	done      chan struct{}
}

func newPacketConn(conn *websocket.Conn, local *net.UDPAddr, pingInterval time.Duration) *packetConn {
	c := &packetConn{
		conn:  conn,
		local: local,
		done:  make(chan struct{}),
	}
	if pingInterval > 0 {
		go c.pingLoop(pingInterval)
	}
	return c
}

func (c *packetConn) pingLoop(interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			c.writeMu.Lock()
			err := c.conn.WriteControl(websocket.PingMessage, nil, time.Now().Add(interval/2))
			c.writeMu.Unlock()
			if err != nil {
				return
			}
		case <-c.done:
			return
		}
	}
}

func (c *packetConn) ReadFrom(p []byte) (int, net.Addr, error) {
	c.readMu.Lock()
	defer c.readMu.Unlock()

	for {
		messageType, frame, err := c.conn.ReadMessage()
		if err != nil {
			return 0, nil, normalizeWebSocketError(err)
		}
		if messageType != websocket.BinaryMessage {
			continue
		}
		source, payload, err := decodeFrame(frame)
		if err != nil {
			return 0, nil, err
		}
		if len(payload) > len(p) {
			return 0, source, io.ErrShortBuffer
		}
		return copy(p, payload), source, nil
	}
}

func (c *packetConn) WriteTo(p []byte, addr net.Addr) (int, error) {
	frame, err := encodeFrame(addr, p)
	if err != nil {
		return 0, err
	}
	c.writeMu.Lock()
	err = c.conn.WriteMessage(websocket.BinaryMessage, frame)
	c.writeMu.Unlock()
	if err != nil {
		return 0, normalizeWebSocketError(err)
	}
	return len(p), nil
}

func (c *packetConn) Close() error {
	var err error
	c.closeOnce.Do(func() {
		close(c.done)
		err = c.conn.Close()
	})
	if err != nil {
		return normalizeWebSocketError(err)
	}
	return nil
}

func (c *packetConn) LocalAddr() net.Addr                { return c.local }
func (c *packetConn) SetDeadline(t time.Time) error      { return c.setDeadlines(t, t) }
func (c *packetConn) SetReadDeadline(t time.Time) error  { return c.conn.SetReadDeadline(t) }
func (c *packetConn) SetWriteDeadline(t time.Time) error { return c.conn.SetWriteDeadline(t) }

func (c *packetConn) setDeadlines(read, write time.Time) error {
	if err := c.conn.SetReadDeadline(read); err != nil {
		return err
	}
	return c.conn.SetWriteDeadline(write)
}
