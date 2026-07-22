package client

// Read reads received data from the buffer, blocking until data is
// available or the connection is closed (returns io.EOF).
// It can be used alongside OnDataChannelMessage – both receive the same data
// unless ReceiveEventsOnly is enabled.
func (c *Client) Read(p []byte) (int, error) {
	return c.recvBuf.Load().Read(p)
}
