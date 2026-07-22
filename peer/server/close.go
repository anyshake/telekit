package server

func (c *Connection) Close() error {
	c.closeOnce.Do(func() {
		c.recvBuf.Close()
		c.resetDataChunk()
		c.resetPendingICE()
		if c.owner != nil {
			if current, ok := c.owner.connections.Get(c.sourceId); ok && current == c {
				c.owner.connections.Del(c.sourceId)
			}
			c.releaseLease()
		}
		pc, dc := c.takePeerConnection()
		if dc != nil {
			c.closeErr = dc.Close()
		}
		if pc != nil {
			if err := pc.Close(); c.closeErr == nil {
				c.closeErr = err
			}
		}
	})
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
	if s.callbackPool != nil {
		s.callbackPool.Close()
	}
	if s.roomLeaseKey != "" {
		roomOwners.CompareAndDelete(s.roomLeaseKey, s)
		s.roomLeaseKey = ""
	}

	return nil
}
