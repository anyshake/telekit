package peer

import (
	"bytes"
	"crypto"
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
)

const (
	HandshakeNonceSize = 32
	HandshakeKeySize   = 32
)

func ServerIDFromPublicKey(publicKey crypto.PublicKey) (string, error) {
	_publicKey, ok := publicKey.(ed25519.PublicKey)
	if !ok {
		return "", errors.New("invalid public key type")
	}
	if len(_publicKey) != ed25519.PublicKeySize {
		return "", errors.New("invalid Ed25519 public key")
	}
	digest := sha256.Sum256(_publicKey)
	return hex.EncodeToString(digest[:]), nil
}

func SignServerHello(
	privateKey ed25519.PrivateKey,
	roomID, clientID, serverID string,
	clientNonce, serverNonce, clientEphemeralKey, serverEphemeralKey, sessionSalt []byte,
) ([]byte, error) {
	if len(privateKey) != ed25519.PrivateKeySize {
		return nil, errors.New("invalid Ed25519 private key")
	}
	canonicalKey := ed25519.NewKeyFromSeed(privateKey[:ed25519.SeedSize])
	if !bytes.Equal(canonicalKey, privateKey) {
		return nil, errors.New("invalid Ed25519 private key")
	}
	if err := validateServerHelloTranscript(roomID, clientID, serverID, clientNonce, serverNonce, clientEphemeralKey, serverEphemeralKey, sessionSalt); err != nil {
		return nil, err
	}
	transcript := serverHelloTranscript(roomID, clientID, serverID, clientNonce, serverNonce, clientEphemeralKey, serverEphemeralKey, sessionSalt)
	return ed25519.Sign(privateKey, transcript), nil
}

func VerifyServerHello(
	publicKey ed25519.PublicKey,
	roomID, clientID, serverID string,
	clientNonce, serverNonce, clientEphemeralKey, serverEphemeralKey, sessionSalt, signature []byte,
) bool {
	if len(publicKey) != ed25519.PublicKeySize || len(signature) != ed25519.SignatureSize {
		return false
	}
	if err := validateServerHelloTranscript(roomID, clientID, serverID, clientNonce, serverNonce, clientEphemeralKey, serverEphemeralKey, sessionSalt); err != nil {
		return false
	}
	transcript := serverHelloTranscript(roomID, clientID, serverID, clientNonce, serverNonce, clientEphemeralKey, serverEphemeralKey, sessionSalt)
	return ed25519.Verify(publicKey, transcript, signature)
}

func validateServerHelloTranscript(
	roomID, clientID, serverID string,
	clientNonce, serverNonce, clientEphemeralKey, serverEphemeralKey, sessionSalt []byte,
) error {
	if roomID == "" || clientID == "" || serverID == "" {
		return errors.New("handshake identities must not be empty")
	}
	if len(clientNonce) != HandshakeNonceSize || len(serverNonce) != HandshakeNonceSize {
		return fmt.Errorf("handshake nonces must be %d bytes", HandshakeNonceSize)
	}
	if len(clientEphemeralKey) != HandshakeKeySize || len(serverEphemeralKey) != HandshakeKeySize {
		return fmt.Errorf("X25519 public keys must be %d bytes", HandshakeKeySize)
	}
	if len(sessionSalt) != HandshakeNonceSize {
		return fmt.Errorf("session salt must be %d bytes", HandshakeNonceSize)
	}
	return nil
}

func serverHelloTranscript(
	roomID, clientID, serverID string,
	clientNonce, serverNonce, clientEphemeralKey, serverEphemeralKey, sessionSalt []byte,
) []byte {
	return encodeHandshakeFields(
		[]byte("telekit/server-hello/v3"),
		[]byte(roomID),
		[]byte(clientID),
		[]byte(serverID),
		clientNonce,
		serverNonce,
		clientEphemeralKey,
		serverEphemeralKey,
		sessionSalt,
	)
}

func encodeHandshakeFields(fields ...[]byte) []byte {
	size := 0
	for _, field := range fields {
		size += 4 + len(field)
	}
	encoded := make([]byte, 0, size)
	var length [4]byte
	for _, field := range fields {
		binary.BigEndian.PutUint32(length[:], uint32(len(field)))
		encoded = append(encoded, length[:]...)
		encoded = append(encoded, field...)
	}
	return encoded
}
