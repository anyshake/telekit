package server

import "github.com/anyshake/telekit/peer"

func (c *Connection) Close() error {
	wasEstablished := false
	c.closeOnce.Do(func() {
		wasEstablished = c.established.Swap(false)
		c.recvBuf.Close()
		if c.owner != nil {
			if current, ok := c.owner.connections.Get(c.sourceId); ok && current == c {
				c.owner.connections.Del(c.sourceId)
			}
			c.releaseLease()
		}
		transportConn := c.transportConnValue()
		if transportConn != nil {
			if c.closeErr == nil {
				c.closeErr = transportConn.Close()
			}
			c.setTransportConn(nil)
		}
		c.stateMu.Lock()
		agent := c.iceAgent
		c.iceAgent = nil
		c.localAddr = peer.Addr{}
		c.remoteAddr = peer.Addr{}
		c.stateMu.Unlock()
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
	s.connections.Clear()
	if s.roomLeaseKey != "" {
		roomOwners.CompareAndDelete(s.roomLeaseKey, s)
		s.roomLeaseKey = ""
	}

	return nil
}
