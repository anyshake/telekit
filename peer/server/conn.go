package server

import (
	"encoding/binary"
	"errors"
	"io"
	"net"
	"time"

	"github.com/anyshake/telekit/peer"
	"github.com/pion/ice/v4"
)

func (c *Connection) LocalAddr() net.Addr {
	c.stateMu.RLock()
	address := c.localAddr
	c.stateMu.RUnlock()
	if address.RoomID == "" {
		return peer.Addr{RoomID: c.roomId, PeerID: c.serverId}
	}
	return address
}

func (c *Connection) RemoteAddr() net.Addr {
	c.stateMu.RLock()
	address := c.remoteAddr
	c.stateMu.RUnlock()
	if address.RoomID == "" {
		return peer.Addr{RoomID: c.roomId, PeerID: c.sourceId}
	}
	return address
}

func (c *Connection) setPhysicalAddrs(local, remote net.Addr) {
	c.stateMu.Lock()
	c.localAddr = peer.AddrFromNet(c.roomId, c.serverId, local)
	c.remoteAddr = peer.AddrFromNet(c.roomId, c.sourceId, remote)
	c.stateMu.Unlock()
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
	if conn := c.transportConnValue(); conn != nil {
		return conn.SetWriteDeadline(t)
	}
	return nil
}

func (c *Connection) writeTimedOut() bool {
	c.deadlineMu.RLock()
	deadline := c.writeDeadline
	c.deadlineMu.RUnlock()
	return !deadline.IsZero() && c.owner.options.GetTimeFunc().After(deadline)
}

func (c *Connection) maxFrameSize() int {
	c.stateMu.RLock()
	transportMaxFrameSize := c.transportMaxFrameSize
	c.stateMu.RUnlock()
	if transportMaxFrameSize > 0 && c.owner.options.MaxFrameSize > transportMaxFrameSize {
		return transportMaxFrameSize
	}
	return c.owner.options.MaxFrameSize
}

func (s *Server) setConnection(conn *Connection) bool {
	s.connectionsMu.Lock()
	defer s.connectionsMu.Unlock()
	select {
	case <-s.closeCh:
		return false
	default:
	}
	if conn == nil || conn.closed.Load() {
		return false
	}
	s.connections.Set(conn.sourceId, conn)
	return true
}

func (c *Connection) isCurrent() bool {
	if c == nil || c.owner == nil || c.closed.Load() {
		return false
	}
	c.owner.connectionsMu.Lock()
	current, ok := c.owner.connections.Get(c.sourceId)
	c.owner.connectionsMu.Unlock()
	return ok && current == c && !c.closed.Load()
}

func (c *Connection) removeCurrent() bool {
	if c == nil || c.owner == nil {
		return false
	}
	c.owner.connectionsMu.Lock()
	current, ok := c.owner.connections.Get(c.sourceId)
	if ok && current == c {
		c.owner.connections.Del(c.sourceId)
	}
	c.owner.connectionsMu.Unlock()
	return ok && current == c
}

func (c *Connection) selectedTransportForSetup() (string, bool) {
	c.stateMu.RLock()
	selected := c.selectedTransport
	available := !c.closed.Load() && c.iceAgent == nil && c.transportConn == nil
	c.stateMu.RUnlock()
	return selected, available && selected != "" && c.isCurrent()
}

func (c *Connection) installICEAgent(agent *ice.Agent) bool {
	if agent == nil {
		return false
	}
	c.stateMu.Lock()
	accepted := !c.closed.Load() && c.iceAgent == nil && c.transportConn == nil && c.isCurrent()
	if accepted {
		c.iceAgent = agent
	}
	c.stateMu.Unlock()
	if !accepted {
		_ = agent.Close()
	}
	return accepted
}

func (c *Connection) ownsICEAgent(agent *ice.Agent) bool {
	c.stateMu.RLock()
	owned := !c.closed.Load() && c.iceAgent == agent
	c.stateMu.RUnlock()
	return owned && c.isCurrent()
}

func (c *Connection) installTransport(agent *ice.Agent, conn net.Conn, maxFrameSize int, local, remote net.Addr) bool {
	if conn == nil {
		return false
	}
	c.stateMu.Lock()
	accepted := !c.closed.Load() && c.iceAgent == agent && c.transportConn == nil && c.dataChannel != nil && c.isCurrent()
	if accepted {
		c.transportConn = conn
		c.transportMaxFrameSize = maxFrameSize
		c.localAddr = peer.AddrFromNet(c.roomId, c.serverId, local)
		c.remoteAddr = peer.AddrFromNet(c.roomId, c.sourceId, remote)
	}
	c.stateMu.Unlock()
	if !accepted {
		_ = conn.Close()
	}
	return accepted
}

func (c *Connection) transportState() (net.Conn, *peer.DataChannel) {
	c.stateMu.RLock()
	conn := c.transportConn
	dataChannel := c.dataChannel
	c.stateMu.RUnlock()
	return conn, dataChannel
}

func (c *Connection) isCurrentTransport(conn net.Conn, dataChannel *peer.DataChannel) bool {
	c.stateMu.RLock()
	current := !c.closed.Load() && c.transportConn == conn && c.dataChannel == dataChannel
	c.stateMu.RUnlock()
	return current && c.isCurrent()
}

func (c *Connection) transportConnValue() net.Conn {
	conn, _ := c.transportState()
	return conn
}

func (c *Connection) readTransport(conn net.Conn, dataChannel *peer.DataChannel, packetMode bool) {
	if packetMode {
		c.readRawTransport(conn, dataChannel)
		return
	}

	var header [8]byte
	var sequenceBytes [peer.DataFrameSequenceSize]byte
	var frame []byte
	for {
		if _, err := io.ReadFull(conn, header[:]); err != nil {
			break
		}
		if !c.isCurrentTransport(conn, dataChannel) {
			break
		}
		length := binary.BigEndian.Uint64(header[:])
		if length == 0 {
			break
		}
		if length > uint64(c.owner.options.MaxFrameSize) {
			break
		}
		if _, err := io.ReadFull(conn, sequenceBytes[:]); err != nil {
			break
		}
		if !c.owner.bufferBudget.Reserve(int(length)) {
			break
		}
		if cap(frame) < int(length) {
			frame = make([]byte, length)
		} else {
			frame = frame[:length]
		}
		if _, err := io.ReadFull(conn, frame); err != nil {
			c.owner.bufferBudget.Release(int(length))
			break
		}
		sequence := binary.BigEndian.Uint64(sequenceBytes[:])
		frameType, raw, err := dataChannel.OpenFrame(sequence, frame, c.owner.options.MaxFrameSize)
		c.owner.bufferBudget.Release(int(length))
		if err != nil || len(raw) > c.owner.options.MaxFrameSize {
			break
		}
		if !c.isCurrentTransport(conn, dataChannel) {
			break
		}
		c.lastTransportRead.Store(c.owner.options.GetTimeFunc().UnixNano())
		if frameType == peer.DataFrameHeartbeat {
			continue
		}
		if err := c.recvBuf.Write(raw); err != nil {
			break
		}
	}
	_ = c.Close()
}

func (c *Connection) readRawTransport(conn net.Conn, dataChannel *peer.DataChannel) {
	packet := make([]byte, c.owner.options.MaxFrameSize+8+peer.DataFrameSequenceSize)
	for {
		n, err := conn.Read(packet)
		if err != nil {
			break
		}
		if n < 8 {
			break
		}
		if !c.isCurrentTransport(conn, dataChannel) {
			break
		}
		length := binary.BigEndian.Uint64(packet[:8])
		if length == 0 {
			break
		}
		if n < 8+peer.DataFrameSequenceSize || length > uint64(c.owner.options.MaxFrameSize) || int(length) != n-8-peer.DataFrameSequenceSize {
			break
		}
		if !c.owner.bufferBudget.Reserve(int(length)) {
			break
		}
		sequenceOffset := 8 + peer.DataFrameSequenceSize
		sequence := binary.BigEndian.Uint64(packet[8:sequenceOffset])
		frameType, raw, err := dataChannel.OpenFrame(sequence, packet[sequenceOffset:n], c.owner.options.MaxFrameSize)
		c.owner.bufferBudget.Release(int(length))
		if errors.Is(err, peer.ErrDataFrameReplay) {
			continue
		}
		if err != nil || len(raw) > c.owner.options.MaxFrameSize {
			break
		}
		if !c.isCurrentTransport(conn, dataChannel) {
			break
		}
		c.lastTransportRead.Store(c.owner.options.GetTimeFunc().UnixNano())
		if frameType == peer.DataFrameHeartbeat {
			continue
		}
		if err := c.recvBuf.Write(raw); err != nil {
			break
		}
	}
	_ = c.Close()
}
