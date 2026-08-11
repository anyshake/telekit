package peer

import (
	"crypto/sha256"
	"errors"
	"fmt"
	"io"

	"golang.org/x/crypto/hkdf"
)

type signalingDirection byte

const (
	signalingClientToServer signalingDirection = iota + 1
	signalingServerToClient
)

// SignalingChannel protects post-handshake signaling with independent keys in
// each direction. A session key therefore has exactly one encrypting codec per
// derived key, even when 96-bit-nonce AEADs are selected.
type SignalingChannel struct {
	sendCodec    *Codec
	receiveCodec *Codec
	sessionKey   []byte
}

func NewSignalingChannel(encryptionType string, sessionKey, additionalData []byte, role DataRole) (*SignalingChannel, error) {
	var sendDirection, receiveDirection signalingDirection
	switch role {
	case DataRoleClient:
		sendDirection = signalingClientToServer
		receiveDirection = signalingServerToClient
	case DataRoleServer:
		sendDirection = signalingServerToClient
		receiveDirection = signalingClientToServer
	default:
		return nil, errors.New("invalid signaling channel role")
	}

	sendKey, err := deriveSignalingKey(sessionKey, sendDirection)
	if err != nil {
		return nil, err
	}
	receiveKey, err := deriveSignalingKey(sessionKey, receiveDirection)
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
	return &SignalingChannel{
		sendCodec:    sendCodec,
		receiveCodec: receiveCodec,
		sessionKey:   append([]byte(nil), sessionKey...),
	}, nil
}

func (c *SignalingChannel) EncodeMessage(message *Message) ([]byte, error) {
	if c == nil || c.sendCodec == nil {
		return nil, errors.New("signaling channel is not initialized")
	}
	return c.sendCodec.EncodeMessage(message)
}

func (c *SignalingChannel) DecodeMessage(data []byte) (*Message, error) {
	if c == nil || c.receiveCodec == nil {
		return nil, errors.New("signaling channel is not initialized")
	}
	return c.receiveCodec.DecodeMessage(data)
}

func (c *SignalingChannel) SessionKey() []byte {
	if c == nil {
		return nil
	}
	return append([]byte(nil), c.sessionKey...)
}

func deriveSignalingKey(sessionKey []byte, direction signalingDirection) ([]byte, error) {
	if len(sessionKey) == 0 {
		return nil, errors.New("session key is empty")
	}
	if direction != signalingClientToServer && direction != signalingServerToClient {
		return nil, errors.New("invalid signaling direction")
	}
	info := encodeHandshakeFields([]byte("telekit/signaling/v1"), []byte{byte(direction)})
	key := make([]byte, HandshakeKeySize)
	if _, err := io.ReadFull(hkdf.New(sha256.New, sessionKey, nil, info), key); err != nil {
		return nil, fmt.Errorf("derive signaling key: %w", err)
	}
	return key, nil
}
