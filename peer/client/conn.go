package client

import (
	"net"
	"time"

	"github.com/anyshake/telekit/peer"
	"github.com/pion/ice/v4"
)

func (c *Client) Close() error { return c.Disconnect() }

func (c *Client) LocalAddr() net.Addr {
	c.stateMu.RLock()
	address := c.localAddr
	c.stateMu.RUnlock()
	if address.RoomID == "" {
		return peer.Addr{RoomID: c.api.RoomId, PeerID: c.clientId}
	}
	return address
}

func (c *Client) RemoteAddr() net.Addr {
	c.stateMu.RLock()
	address := c.remoteAddr
	c.stateMu.RUnlock()
	if address.RoomID == "" {
		return peer.Addr{RoomID: c.api.RoomId, PeerID: c.serverId}
	}
	return address
}

func (c *Client) setPhysicalAddrs(local, remote net.Addr) {
	c.stateMu.Lock()
	c.localAddr = peer.AddrFromNet(c.api.RoomId, c.clientId, local)
	c.remoteAddr = peer.AddrFromNet(c.api.RoomId, c.serverId, remote)
	c.stateMu.Unlock()
}

func (c *Client) clearPhysicalAddrs() {
	c.stateMu.Lock()
	c.localAddr = peer.Addr{}
	c.remoteAddr = peer.Addr{}
	c.stateMu.Unlock()
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
	if conn := c.transportConnValue(); conn != nil {
		return conn.SetWriteDeadline(t)
	}
	return nil
}

func (c *Client) writeTimedOut() bool {
	c.deadlineMu.RLock()
	deadline := c.writeDeadline
	c.deadlineMu.RUnlock()
	return !deadline.IsZero() && c.options.GetTimeFunc().After(deadline)
}

func (c *Client) installICEAgent(agent *ice.Agent) bool {
	if agent == nil {
		return false
	}
	c.stateMu.Lock()
	accepted := !c.manualDisconnect.Load() && c.iceAgent == nil && c.transportConn == nil
	if accepted {
		c.iceAgent = agent
	}
	c.stateMu.Unlock()
	if !accepted {
		_ = agent.Close()
	}
	return accepted
}

func (c *Client) setTransportConn(agent *ice.Agent, conn net.Conn, dataChannel *peer.DataChannel) bool {
	if conn == nil {
		return false
	}
	c.stateMu.Lock()
	accepted := !c.manualDisconnect.Load() && c.iceAgent == agent && c.transportConn == nil && dataChannel != nil
	if accepted {
		c.transportConn = conn
		c.dataChannel = dataChannel
	}
	c.stateMu.Unlock()
	if !accepted {
		_ = conn.Close()
	}
	return accepted
}

func (c *Client) transportState() (net.Conn, *peer.DataChannel) {
	c.stateMu.RLock()
	conn := c.transportConn
	dataChannel := c.dataChannel
	c.stateMu.RUnlock()
	return conn, dataChannel
}

func (c *Client) isCurrentTransport(conn net.Conn, dataChannel *peer.DataChannel) bool {
	c.stateMu.RLock()
	current := c.transportConn == conn && c.dataChannel == dataChannel
	c.stateMu.RUnlock()
	return current
}

// finishTransport detaches only conn's transport generation. A reader from an
// old connection must not tear down a newer connection established after a
// reconnect.
func (c *Client) finishTransport(conn net.Conn, dataChannel *peer.DataChannel) bool {
	c.stateMu.Lock()
	if c.transportConn != conn || c.dataChannel != dataChannel {
		c.stateMu.Unlock()
		return false
	}
	c.transportConn = nil
	c.dataChannel = nil
	agent := c.iceAgent
	c.iceAgent = nil
	c.localAddr = peer.Addr{}
	c.remoteAddr = peer.Addr{}
	c.stateMu.Unlock()

	c.setIsConnected(false)
	c.recvBuf.Load().Reset()
	_ = conn.Close()
	if agent != nil {
		_ = agent.Close()
	}
	reconnectGeneration := c.reconnectGeneration.Add(1)
	if c.options.OnDisconnected != nil {
		c.options.OnDisconnected(c)
	}
	if c.manualDisconnect.Load() {
		return true
	}
	go c.reconnectAfterTransportFailure(reconnectGeneration)
	return true
}

func (c *Client) transportConnValue() net.Conn {
	conn, _ := c.transportState()
	return conn
}
