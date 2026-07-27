package server

import (
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
	c.established.Store(true)
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
