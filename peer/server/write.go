package server

import (
	"encoding/binary"
	"errors"
	"io"
	"net"
	"os"

	"github.com/anyshake/telekit/peer"
)

func (c *Connection) sendHeartbeat(expectedConn net.Conn, expectedDataChannel *peer.DataChannel) error {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	conn, dataChannel := c.transportState()
	if conn == nil || conn != expectedConn || dataChannel != expectedDataChannel {
		return errors.New("transport not connected")
	}
	sequence, ciphertext, err := dataChannel.SealHeartbeat()
	if err != nil {
		return err
	}
	frame := make([]byte, 8+peer.DataFrameSequenceSize+len(ciphertext))
	binary.BigEndian.PutUint64(frame[:8], uint64(len(ciphertext)))
	binary.BigEndian.PutUint64(frame[8:8+peer.DataFrameSequenceSize], sequence)
	copy(frame[8+peer.DataFrameSequenceSize:], ciphertext)
	n, err := conn.Write(frame)
	if err != nil {
		return err
	}
	if n != len(frame) {
		return io.ErrShortWrite
	}
	return nil
}

func (c *Connection) Write(p []byte) (int, error) {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	originalLen := len(p)
	conn, dataChannel := c.transportState()
	if conn == nil || dataChannel == nil || c.closed.Load() {
		return 0, errors.New("transport not connected")
	}
	if c.writeTimedOut() {
		return 0, os.ErrDeadlineExceeded
	}
	if originalLen == 0 {
		return 0, nil
	}

	frameLimit := c.maxFrameSize()
	for plaintextOffset := 0; plaintextOffset < originalLen; {
		plaintextEnd := min(plaintextOffset+frameLimit-64, originalLen)
		var sequence uint64
		var ciphertext []byte
		for {
			var err error
			sequence, ciphertext, err = dataChannel.Seal(p[plaintextOffset:plaintextEnd])
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
		frame := make([]byte, 8+peer.DataFrameSequenceSize+len(ciphertext))
		binary.BigEndian.PutUint64(frame[:8], uint64(len(ciphertext)))
		binary.BigEndian.PutUint64(frame[8:8+peer.DataFrameSequenceSize], sequence)
		copy(frame[8+peer.DataFrameSequenceSize:], ciphertext)

		if _, err := conn.Write(frame); err != nil {
			return plaintextOffset, err
		}
		plaintextOffset = plaintextEnd
	}

	return originalLen, nil
}
