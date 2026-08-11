package client

import (
	"encoding/binary"
	"errors"
	"io"
	"os"
)

func (c *Client) sendHeartbeat() error {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	conn := c.transportConnValue()
	if conn == nil {
		return errors.New("transport not connected")
	}
	var frame [8]byte
	n, err := conn.Write(frame[:])
	if err != nil {
		return err
	}
	if n != len(frame) {
		return io.ErrShortWrite
	}
	return nil
}

func (c *Client) Write(p []byte) (int, error) {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	originalLen := len(p)
	conn := c.transportConnValue()
	if !c.isConnected() || conn == nil {
		return 0, errors.New("transport not connected")
	}
	if c.writeTimedOut() {
		return 0, os.ErrDeadlineExceeded
	}
	if originalLen == 0 {
		return 0, nil
	}

	frameLimit := c.options.MaxFrameSize
	if transportLimit := transportMaxFrameSize(c.options.Transport); transportLimit > 0 && frameLimit > transportLimit {
		frameLimit = transportLimit
	}
	for plaintextOffset := 0; plaintextOffset < originalLen; {
		plaintextEnd := min(plaintextOffset+frameLimit-64, originalLen)
		var ciphertext []byte
		for {
			var err error
			ciphertext, err = c.codec.EncodeWithEncryption(p[plaintextOffset:plaintextEnd])
			if err != nil {
				return plaintextOffset, err
			}
			if len(ciphertext) <= frameLimit {
				break
			}
			if plaintextEnd-plaintextOffset <= 1 {
				return plaintextOffset, errors.New("encoded frame exceeds configured limit")
			}
			plaintextEnd = plaintextOffset + (plaintextEnd-plaintextOffset)/2
		}
		header := make([]byte, 8)
		binary.BigEndian.PutUint64(header, uint64(len(ciphertext)))
		frame := append(header, ciphertext...)

		if _, err := conn.Write(frame); err != nil {
			return plaintextOffset, err
		}
		plaintextOffset = plaintextEnd
	}

	return originalLen, nil
}
