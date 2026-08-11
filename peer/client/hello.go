package client

import (
	"bytes"
	"crypto/ecdh"
	"crypto/rand"
	"errors"
	"time"

	"github.com/anyshake/telekit/peer"
)

type serverHandshake struct {
	serverID   string
	codec      *peer.Codec
	transports []string
}

type clientHandshake struct {
	nonce      []byte
	privateKey *ecdh.PrivateKey
	publicKey  []byte
	message    []byte
}

func (c *Client) handleServerHello(handshakeCodec *peer.Codec, data []byte, attempt *clientHandshake) (*serverHandshake, error) {
	msg, err := handshakeCodec.DecodeMessage(data)
	if err != nil {
		return nil, err
	}

	if msg.Header.SourceId == "" || msg.Header.TargetId != c.clientId || msg.Header.Type != peer.MessageTypeServerHello {
		return nil, errors.New("unexpected server hello")
	}
	expectedServerID, err := peer.ServerIDFromPublicKey(c.psk.ServerPublicKey)
	if err != nil {
		return nil, err
	}
	if msg.Header.SourceId != expectedServerID {
		return nil, errors.New("server identity does not match pinned public key")
	}
	if len(msg.Payload.SessionSalt) != peer.HandshakeNonceSize || len(msg.Payload.ServerNonce) != peer.HandshakeNonceSize {
		return nil, errors.New("invalid server handshake values")
	}
	if !bytes.Equal(msg.Payload.ClientNonce, attempt.nonce) {
		return nil, errors.New("server hello is not bound to the current client hello")
	}
	if !bytes.Equal(msg.Payload.ClientEphemeralKey, attempt.publicKey) {
		return nil, errors.New("server hello is not bound to the current client key")
	}
	if !peer.VerifyServerHello(
		c.psk.ServerPublicKey,
		c.api.RoomId,
		c.clientId,
		msg.Header.SourceId,
		msg.Payload.ClientNonce,
		msg.Payload.ServerNonce,
		msg.Payload.ClientEphemeralKey,
		msg.Payload.ServerEphemeralKey,
		msg.Payload.SessionSalt,
		msg.Payload.Signature,
	) {
		return nil, errors.New("invalid server identity signature")
	}
	serverPublicKey, err := ecdh.X25519().NewPublicKey(msg.Payload.ServerEphemeralKey)
	if err != nil {
		return nil, errors.New("invalid server X25519 public key")
	}
	sharedSecret, err := attempt.privateKey.ECDH(serverPublicKey)
	if err != nil {
		return nil, errors.New("invalid server X25519 public key")
	}

	encType, masterKey := handshakeCodec.GetSecret()
	sessionKey, err := peer.DeriveSessionKey(
		masterKey,
		sharedSecret,
		msg.Payload.SessionSalt,
		c.api.RoomId,
		c.clientId,
		msg.Header.SourceId,
		attempt.nonce,
		msg.Payload.ServerNonce,
		attempt.publicKey,
		msg.Payload.ServerEphemeralKey,
	)
	if err != nil {
		return nil, err
	}
	sessionCodec, err := peer.NewCodec(encType, sessionKey, c.options.EncryptionAAD, c.options.UseCompression)
	if err != nil {
		return nil, err
	}

	return &serverHandshake{serverID: msg.Header.SourceId, codec: sessionCodec, transports: append([]string(nil), msg.Payload.Transports...)}, nil
}

func (c *Client) buildClientHello(handshakeCodec *peer.Codec) (*clientHandshake, error) {
	nonce := make([]byte, peer.HandshakeNonceSize)
	if _, err := rand.Read(nonce); err != nil {
		return nil, err
	}
	privateKey, err := ecdh.X25519().GenerateKey(rand.Reader)
	if err != nil {
		return nil, err
	}
	publicKey := privateKey.PublicKey().Bytes()

	dataBytes, err := handshakeCodec.EncodeMessage(&peer.Message{
		Header: &peer.Header{
			SourceId: c.clientId,
			Type:     peer.MessageTypeClientHello,
		},
		Payload: &peer.Payload{
			ClientNonce:        nonce,
			ClientEphemeralKey: publicKey,
			HandshakeRoomID:    c.api.RoomId,
			HandshakeClientID:  c.clientId,
			Timestamp:          []byte(c.options.GetTimeFunc().UTC().Format(time.RFC3339Nano)),
		},
	})
	if err != nil {
		return nil, err
	}

	return &clientHandshake{
		nonce:      nonce,
		privateKey: privateKey,
		publicKey:  publicKey,
		message:    dataBytes,
	}, nil
}
