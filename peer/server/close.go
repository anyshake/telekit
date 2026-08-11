package server

import "github.com/anyshake/telekit/peer"

func (c *Connection) Close() error {
	c.closed.Store(true)
	wasEstablished := false
	c.closeOnce.Do(func() {
		wasEstablished = c.established.Swap(false)
		if c.recvBuf != nil {
			c.recvBuf.Close()
		}
		if c.owner != nil {
			c.removeCurrent()
			c.releaseLease()
		}
		c.stateMu.Lock()
		transportConn := c.transportConn
		c.transportConn = nil
		c.dataChannel = nil
		agent := c.iceAgent
		c.iceAgent = nil
		c.localAddr = peer.Addr{}
		c.remoteAddr = peer.Addr{}
		c.stateMu.Unlock()
		if transportConn != nil {
			c.closeErr = transportConn.Close()
		}
		if agent != nil {
			if err := agent.Close(); c.closeErr == nil {
				c.closeErr = err
			}
		}
	})
	if wasEstablished && c.owner != nil && c.owner.options.OnDisconnected != nil {
		c.owner.options.OnDisconnected(c)
	}
	return c.closeErr
}

func (s *Server) Close() error {
	s.closeOnce.Do(func() { close(s.closeCh) })
	if s.onHandshake != nil {
		_ = s.onHandshake.Unsubscribe()
		s.onHandshake = nil
	}

	if s.onOffer != nil {
		_ = s.onOffer.Unsubscribe()
		s.onOffer = nil
	}

	for _, conn := range s.connections.Iterator() {
		_ = conn.Close()
	}
	s.connectionsMu.Lock()
	s.connections.Clear()
	s.connectionsMu.Unlock()
	if s.roomLeaseKey != "" {
		roomOwners.CompareAndDelete(s.roomLeaseKey, s)
		s.roomLeaseKey = ""
	}

	return nil
}
