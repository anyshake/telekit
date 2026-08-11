package server

import (
	"encoding/binary"
	"io"
	"net"
	"time"

	"github.com/anyshake/telekit/peer"
)

const (
	transportKeepaliveInterval = 5 * time.Second
	transportKeepaliveTimeout  = 15 * time.Second
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

func (c *Connection) setTransportConn(conn net.Conn) {
	c.stateMu.Lock()
	c.transportConn = conn
	c.stateMu.Unlock()
}

func (c *Connection) transportConnValue() net.Conn {
	c.stateMu.RLock()
	conn := c.transportConn
	c.stateMu.RUnlock()
	return conn
}

func (c *Connection) readTransport(conn net.Conn, packetMode bool) {
	if packetMode {
		c.readRawTransport(conn)
		return
	}

	var header [8]byte
	var frame []byte
	for {
		if _, err := io.ReadFull(conn, header[:]); err != nil {
			break
		}
		length := binary.BigEndian.Uint64(header[:])
		if length == 0 {
			c.lastTransportRead.Store(c.owner.options.GetTimeFunc().UnixNano())
			if err := c.sendHeartbeat(); err != nil {
				break
			}
			continue
		}
		if length > uint64(c.owner.options.MaxFrameSize) {
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
		c.lastTransportRead.Store(c.owner.options.GetTimeFunc().UnixNano())
		raw, err := c.codec.DecodeWithDecryptionLimit(frame, c.owner.options.MaxFrameSize)
		c.owner.bufferBudget.Release(int(length))
		if err != nil || len(raw) > c.owner.options.MaxFrameSize {
			break
		}
		if err := c.recvBuf.Write(raw); err != nil {
			break
		}
	}
	_ = c.Close()
}

func (c *Connection) readRawTransport(conn net.Conn) {
	packet := make([]byte, c.owner.options.MaxFrameSize+8)
	for {
		n, err := conn.Read(packet)
		if err != nil {
			break
		}
		if n < 8 {
			break
		}
		length := binary.BigEndian.Uint64(packet[:8])
		if length == 0 {
			if n != 8 {
				break
			}
			c.lastTransportRead.Store(c.owner.options.GetTimeFunc().UnixNano())
			if err := c.sendHeartbeat(); err != nil {
				break
			}
			continue
		}
		if length > uint64(c.owner.options.MaxFrameSize) || int(length) != n-8 {
			break
		}
		c.lastTransportRead.Store(c.owner.options.GetTimeFunc().UnixNano())
		if !c.owner.bufferBudget.Reserve(int(length)) {
			break
		}
		raw, err := c.codec.DecodeWithDecryptionLimit(packet[8:n], c.owner.options.MaxFrameSize)
		c.owner.bufferBudget.Release(int(length))
		if err != nil || len(raw) > c.owner.options.MaxFrameSize {
			break
		}
		if err := c.recvBuf.Write(raw); err != nil {
			break
		}
	}
	_ = c.Close()
}
