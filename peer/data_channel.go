package peer

import (
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
	"sync/atomic"

	"golang.org/x/crypto/hkdf"
)

const DataFrameSequenceSize = 8

var ErrDataFrameReplay = errors.New("data frame sequence was replayed")
var ErrUnexpectedDataFrameType = errors.New("unexpected data frame type")

type DataFrameType byte

const (
	DataFrameApplication DataFrameType = iota + 1
	DataFrameHeartbeat
)

type DataRole byte

const (
	DataRoleClient DataRole = iota + 1
	DataRoleServer
)

type dataDirection byte

const (
	dataClientToServer dataDirection = iota + 1
	dataServerToClient
)

// DataChannel protects one bidirectional application-data session. Each
// direction has an independent key and sequence space, preventing reflection
// and allowing the receiver to reject replayed frames.
type DataChannel struct {
	sendCodec        *Codec
	receiveCodec     *Codec
	sendDirection    dataDirection
	receiveDirection dataDirection
	sendSequence     atomic.Uint64
	receiveWindow    ReplayWindow
}

func NewDataChannel(encryptionType string, sessionKey, additionalData []byte, role DataRole) (*DataChannel, error) {
	var sendDirection, receiveDirection dataDirection
	switch role {
	case DataRoleClient:
		sendDirection = dataClientToServer
		receiveDirection = dataServerToClient
	case DataRoleServer:
		sendDirection = dataServerToClient
		receiveDirection = dataClientToServer
	default:
		return nil, errors.New("invalid data channel role")
	}

	sendKey, err := deriveDataKey(sessionKey, sendDirection)
	if err != nil {
		return nil, err
	}
	receiveKey, err := deriveDataKey(sessionKey, receiveDirection)
	if err != nil {
		return nil, err
	}
	sendCodec, err := NewCodec(encryptionType, sendKey, additionalData)
	if err != nil {
		return nil, err
	}
	receiveCodec, err := NewCodec(encryptionType, receiveKey, additionalData)
	if err != nil {
		return nil, err
	}
	return &DataChannel{
		sendCodec:        sendCodec,
		receiveCodec:     receiveCodec,
		sendDirection:    sendDirection,
		receiveDirection: receiveDirection,
	}, nil
}

func (c *DataChannel) Seal(plaintext []byte) (uint64, []byte, error) {
	return c.sealFrame(DataFrameApplication, plaintext)
}

func (c *DataChannel) SealHeartbeat() (uint64, []byte, error) {
	return c.sealFrame(DataFrameHeartbeat, nil)
}

func (c *DataChannel) sealFrame(frameType DataFrameType, plaintext []byte) (uint64, []byte, error) {
	if c == nil || c.sendCodec == nil {
		return 0, nil, errors.New("data channel is not initialized")
	}
	if frameType != DataFrameApplication && frameType != DataFrameHeartbeat {
		return 0, nil, ErrUnexpectedDataFrameType
	}
	frame := make([]byte, 1+len(plaintext))
	frame[0] = byte(frameType)
	copy(frame[1:], plaintext)
	var sequence uint64
	for {
		current := c.sendSequence.Load()
		if current == math.MaxUint64 {
			return 0, nil, errors.New("data frame sequence exhausted")
		}
		sequence = current + 1
		if c.sendSequence.CompareAndSwap(current, sequence) {
			break
		}
	}
	ciphertext, err := c.sendCodec.encodeWithEncryption(frame, aadDomainData, dataFrameContext(c.sendDirection, sequence))
	if err != nil {
		return 0, nil, err
	}
	return sequence, ciphertext, nil
}

func (c *DataChannel) Open(sequence uint64, ciphertext []byte, maxDecodedSize int) ([]byte, error) {
	frameType, plaintext, err := c.OpenFrame(sequence, ciphertext, maxDecodedSize)
	if err != nil {
		return nil, err
	}
	if frameType != DataFrameApplication {
		return nil, ErrUnexpectedDataFrameType
	}
	return plaintext, nil
}

func (c *DataChannel) OpenFrame(sequence uint64, ciphertext []byte, maxDecodedSize int) (DataFrameType, []byte, error) {
	if c == nil || c.receiveCodec == nil {
		return 0, nil, errors.New("data channel is not initialized")
	}
	if sequence == 0 {
		return 0, nil, errors.New("data frame sequence is zero")
	}
	frame, err := c.receiveCodec.decodeWithDecryption(
		ciphertext,
		aadDomainData,
		dataFrameContext(c.receiveDirection, sequence),
		maxDecodedSize,
	)
	if err != nil {
		return 0, nil, err
	}
	if len(frame) == 0 {
		return 0, nil, ErrUnexpectedDataFrameType
	}
	frameType := DataFrameType(frame[0])
	if frameType != DataFrameApplication && frameType != DataFrameHeartbeat {
		return 0, nil, ErrUnexpectedDataFrameType
	}
	if frameType == DataFrameHeartbeat && len(frame) != 1 {
		return 0, nil, ErrUnexpectedDataFrameType
	}
	// Authenticate before advancing the window. Otherwise an attacker could
	// send a forged high sequence number and suppress legitimate frames.
	if !c.receiveWindow.Accept(sequence) {
		return 0, nil, ErrDataFrameReplay
	}
	return frameType, frame[1:], nil
}

func deriveDataKey(sessionKey []byte, direction dataDirection) ([]byte, error) {
	if len(sessionKey) == 0 {
		return nil, errors.New("session key is empty")
	}
	if direction != dataClientToServer && direction != dataServerToClient {
		return nil, errors.New("invalid data direction")
	}
	info := encodeHandshakeFields([]byte("telekit/data/v1"), []byte{byte(direction)})
	key := make([]byte, 32)
	if _, err := io.ReadFull(hkdf.New(sha256.New, sessionKey, nil, info), key); err != nil {
		return nil, fmt.Errorf("derive data key: %w", err)
	}
	return key, nil
}

func dataFrameContext(direction dataDirection, sequence uint64) []byte {
	context := make([]byte, 1+DataFrameSequenceSize)
	context[0] = byte(direction)
	binary.BigEndian.PutUint64(context[1:], sequence)
	return context
}
