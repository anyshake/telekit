package server

import (
	"net"
	"time"

	"github.com/anyshake/telekit/peer"
	"github.com/pion/webrtc/v4"
)

var _ net.Conn = (*Connection)(nil)

func (c *Connection) LocalAddr() net.Addr {
	return peer.Addr{RoomID: c.roomId, PeerID: c.serverId}
}

func (c *Connection) RemoteAddr() net.Addr {
	return peer.Addr{RoomID: c.roomId, PeerID: c.sourceId}
}

func (c *Connection) SetDeadline(t time.Time) error {
	c.recvBuf.SetDeadline(t)
	return c.SetWriteDeadline(t)
}

func (c *Connection) SetReadDeadline(t time.Time) error {
	c.recvBuf.SetDeadline(t)
	return nil
}

func (c *Connection) SetWriteDeadline(t time.Time) error {
	c.deadlineMu.Lock()
	c.writeDeadline = t
	c.deadlineMu.Unlock()
	return nil
}

func (c *Connection) writeTimedOut() bool {
	c.deadlineMu.RLock()
	deadline := c.writeDeadline
	c.deadlineMu.RUnlock()
	return !deadline.IsZero() && time.Now().After(deadline)
}

func (c *Connection) setDataChannel(dc *webrtc.DataChannel) {
	c.stateMu.Lock()
	c.dc = dc
	c.stateMu.Unlock()
}

func (c *Connection) dataChannel() *webrtc.DataChannel {
	c.stateMu.RLock()
	dc := c.dc
	c.stateMu.RUnlock()
	return dc
}

func (c *Connection) peerConnection() *webrtc.PeerConnection {
	c.stateMu.RLock()
	pc := c.pc
	c.stateMu.RUnlock()
	return pc
}

func (c *Connection) takePeerConnection() (*webrtc.PeerConnection, *webrtc.DataChannel) {
	c.stateMu.Lock()
	pc, dc := c.pc, c.dc
	c.pc, c.dc = nil, nil
	c.stateMu.Unlock()
	return pc, dc
}
