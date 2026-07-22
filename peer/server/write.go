package server

import (
	"encoding/binary"
	"errors"
	"os"
	"time"

	"github.com/pion/webrtc/v4"
)

func (c *Connection) Write(p []byte) (int, error) {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	originalLen := len(p)
	if originalLen == 0 {
		return 0, nil
	}
	dc := c.dataChannel()
	if dc == nil {
		return 0, errors.New("data channel not connected")
	}
	if c.writeTimedOut() {
		return 0, os.ErrDeadlineExceeded
	}

	for plaintextOffset := 0; plaintextOffset < originalLen; {
		plaintextEnd := min(plaintextOffset+c.owner.options.MaxFrameSize-64, originalLen)
		var ciphertext []byte
		for {
			var err error
			ciphertext, err = c.codec.EncodeWithEncryption(p[plaintextOffset:plaintextEnd])
			if err != nil {
				return plaintextOffset, err
			}
			if len(ciphertext) <= c.owner.options.MaxFrameSize {
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

		for offset := 0; offset < len(frame); {
			if c.writeTimedOut() {
				return plaintextOffset, os.ErrDeadlineExceeded
			}
			if dc.ReadyState() != webrtc.DataChannelStateOpen {
				return plaintextOffset, errors.New("data channel closed")
			}
			end := min(offset+MAX_CHUNK_SIZE, len(frame))
			if err := c.waitForSendCapacity(dc, end-offset); err != nil {
				return plaintextOffset, err
			}
			if err := dc.Send(frame[offset:end]); err != nil {
				return plaintextOffset, err
			}
			offset = end
		}
		plaintextOffset = plaintextEnd
	}

	return originalLen, nil
}

func (c *Connection) waitForSendCapacity(dc *webrtc.DataChannel, next int) error {
	for dc.BufferedAmount()+uint64(next) > uint64(c.owner.options.MaxSendBufferSize) {
		if c.writeTimedOut() {
			return os.ErrDeadlineExceeded
		}
		if dc.ReadyState() != webrtc.DataChannelStateOpen {
			return errors.New("data channel closed")
		}
		time.Sleep(time.Millisecond)
	}
	return nil
}
