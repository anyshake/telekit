package client

import (
	"net"
	"time"

	"github.com/anyshake/telekit/peer"
	"github.com/pion/webrtc/v4"
)

var _ net.Conn = (*Client)(nil)

func (c *Client) Close() error { return c.Disconnect() }

func (c *Client) LocalAddr() net.Addr {
	return peer.Addr{RoomID: c.api.RoomId, PeerID: c.clientId}
}

func (c *Client) RemoteAddr() net.Addr {
	return peer.Addr{RoomID: c.api.RoomId, PeerID: c.serverId}
}

func (c *Client) SetDeadline(t time.Time) error {
	c.recvBuf.Load().SetDeadline(t)
	return c.SetWriteDeadline(t)
}

func (c *Client) SetReadDeadline(t time.Time) error {
	c.recvBuf.Load().SetDeadline(t)
	return nil
}

func (c *Client) SetWriteDeadline(t time.Time) error {
	c.deadlineMu.Lock()
	c.writeDeadline = t
	c.deadlineMu.Unlock()
	return nil
}

func (c *Client) writeTimedOut() bool {
	c.deadlineMu.RLock()
	deadline := c.writeDeadline
	c.deadlineMu.RUnlock()
	return !deadline.IsZero() && time.Now().After(deadline)
}

func (c *Client) setPeerConnection(pc *webrtc.PeerConnection, dc *webrtc.DataChannel) {
	c.stateMu.Lock()
	c.pc, c.dc = pc, dc
	c.stateMu.Unlock()
}

func (c *Client) dataChannel() *webrtc.DataChannel {
	c.stateMu.RLock()
	dc := c.dc
	c.stateMu.RUnlock()
	return dc
}

func (c *Client) peerConnection() *webrtc.PeerConnection {
	c.stateMu.RLock()
	pc := c.pc
	c.stateMu.RUnlock()
	return pc
}

func (c *Client) takePeerConnection() (*webrtc.PeerConnection, *webrtc.DataChannel) {
	c.stateMu.Lock()
	pc, dc := c.pc, c.dc
	c.pc, c.dc = nil, nil
	c.stateMu.Unlock()
	return pc, dc
}
