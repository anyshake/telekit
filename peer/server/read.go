package server

// Read reads received data from the buffer, blocking until data is available
// or the connection is closed (returns io.EOF).
func (c *Connection) Read(p []byte) (int, error) {
	return c.recvBuf.Read(p)
}
