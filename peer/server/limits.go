package server

import (
	"bytes"
	"sync/atomic"
	"time"
)

func reserveCounter(counter *atomic.Int64, limit int) bool {
	for {
		current := counter.Load()
		if current >= int64(limit) {
			return false
		}
		if counter.CompareAndSwap(current, current+1) {
			return true
		}
	}
}

func (s *Server) reserveConnection() bool {
	if !reserveCounter(&s.totalConnections, s.options.MaxConnections) {
		return false
	}
	if !reserveCounter(&s.pendingHandshakes, s.options.MaxPendingHandshakes) {
		s.totalConnections.Add(-1)
		return false
	}
	return true
}

func (c *Connection) markEstablished() {
	if c.pendingLease.Swap(false) {
		c.owner.pendingHandshakes.Add(-1)
	}
	c.stopHandshakeTimer()
}

func (c *Connection) releaseLease() {
	c.stopHandshakeTimer()
	if c.pendingLease.Swap(false) {
		c.owner.pendingHandshakes.Add(-1)
	}
	if c.totalLease.Swap(false) {
		c.owner.totalConnections.Add(-1)
	}
}

func (c *Connection) startHandshakeTimer(timeout time.Duration) {
	c.handshakeMu.Lock()
	if c.handshakeTimer == nil {
		c.handshakeTimer = time.AfterFunc(timeout, func() {
			if !c.pendingLease.Load() {
				return
			}
			if current, ok := c.owner.connections.Get(c.sourceId); ok && current == c {
				c.owner.connections.Del(c.sourceId)
			}
			_ = c.Close()
		})
	}
	c.handshakeMu.Unlock()
}

func (c *Connection) stopHandshakeTimer() {
	c.handshakeMu.Lock()
	if c.handshakeTimer != nil {
		c.handshakeTimer.Stop()
		c.handshakeTimer = nil
	}
	c.handshakeMu.Unlock()
}

func (s *Server) submitDataCallback(conn *Connection, data []byte) bool {
	if s.callbackPool == nil || !s.bufferBudget.Reserve(len(data)) {
		return false
	}
	release := func() { s.bufferBudget.Release(len(data)) }
	ok := s.callbackPool.SubmitWithCancel(func() {
		defer release()
		s.options.OnDataChannelMessage(conn, data)
	}, release)
	if !ok {
		release()
	}
	return ok
}

func (c *Connection) resetDataChunkLocked() {
	if c.dataChunkBuf.reserved > 0 && c.owner != nil {
		c.owner.bufferBudget.Release(c.dataChunkBuf.reserved)
	}
	c.dataChunkBuf.reserved = 0
	c.dataChunkBuf.expectedLen = 0
	c.dataChunkBuf.recvBuffer = bytes.Buffer{}
}

func (c *Connection) resetDataChunk() {
	c.dataChunkBuf.mu.Lock()
	c.resetDataChunkLocked()
	c.dataChunkBuf.mu.Unlock()
}

func (c *Connection) resetPendingICE() {
	c.signalMu.Lock()
	if c.pendingICEBytes > 0 && c.owner != nil {
		c.owner.bufferBudget.Release(c.pendingICEBytes)
	}
	c.pendingICE = nil
	c.pendingICEBytes = 0
	c.signalMu.Unlock()
}
