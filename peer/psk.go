package peer

import (
	"crypto/ed25519"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"regexp"

	"golang.org/x/crypto/hkdf"
)

const MinPreSharedKeySize = 32
const MaxClientIDLength = 128

var ErrUnknownClient = errors.New("unknown client")
var clientIDPattern = regexp.MustCompile(`^[A-Za-z0-9_-]+$`)

// PreSharedKey couples the public client identity used for routing with its
// secret authentication key and the pinned server identity. Key should contain
// at least 256 bits of random material; it is never sent over signaling.
type PreSharedKey struct {
	ClientID string
	Key      []byte
	// ServerPublicKey pins the Ed25519 identity authorized to answer this
	// client's handshake.
	ServerPublicKey ed25519.PublicKey
}

func (p PreSharedKey) Validate() error {
	if err := ValidateClientCredentials(p.ClientID, p.Key); err != nil {
		return err
	}
	if len(p.ServerPublicKey) != ed25519.PublicKeySize {
		return fmt.Errorf("server public key must be %d bytes", ed25519.PublicKeySize)
	}
	return nil
}

func ValidateClientCredentials(clientID string, key []byte) error {
	if err := ValidateClientID(clientID); err != nil {
		return err
	}
	if len(key) < MinPreSharedKeySize {
		return fmt.Errorf("pre-shared key must be at least %d bytes", MinPreSharedKeySize)
	}
	return nil
}

func ValidateClientID(clientID string) error {
	if len(clientID) == 0 || len(clientID) > MaxClientIDLength {
		return fmt.Errorf("client ID must be between 1 and %d bytes", MaxClientIDLength)
	}
	if !clientIDPattern.MatchString(clientID) {
		return errors.New("client ID must use letters, digits, underscore, or hyphen")
	}
	return nil
}

// KeyProvider resolves client identities without exposing a peer ID to the
// client-facing connection API.
type KeyProvider interface {
	Key(clientID string) ([]byte, error)
}

type KeyProviderFunc func(clientID string) ([]byte, error)

func (f KeyProviderFunc) Key(clientID string) ([]byte, error) { return f(clientID) }

// StaticKeyring is a convenient KeyProvider for small deployments.
type StaticKeyring map[string][]byte

func (k StaticKeyring) Key(clientID string) ([]byte, error) {
	key, ok := k[clientID]
	if !ok {
		return nil, ErrUnknownClient
	}
	return append([]byte(nil), key...), nil
}

// DeriveSessionKey creates a key scoped to one room, client, pinned server,
// and handshake. The salt is freshly generated for every successful handshake.
func DeriveSessionKey(
	masterKey, sharedSecret, salt []byte,
	roomID, clientID, serverID string,
	clientNonce, serverNonce, clientEphemeralKey, serverEphemeralKey []byte,
) ([]byte, error) {
	if err := ValidateClientCredentials(clientID, masterKey); err != nil {
		return nil, err
	}
	if roomID == "" || serverID == "" {
		return nil, errors.New("room ID and server ID must not be empty")
	}
	if len(salt) != HandshakeNonceSize {
		return nil, fmt.Errorf("session salt must be %d bytes", HandshakeNonceSize)
	}
	if len(clientNonce) != HandshakeNonceSize || len(serverNonce) != HandshakeNonceSize {
		return nil, fmt.Errorf("handshake nonces must be %d bytes", HandshakeNonceSize)
	}
	if len(sharedSecret) != HandshakeKeySize {
		return nil, fmt.Errorf("X25519 shared secret must be %d bytes", HandshakeKeySize)
	}
	if len(clientEphemeralKey) != HandshakeKeySize || len(serverEphemeralKey) != HandshakeKeySize {
		return nil, fmt.Errorf("X25519 public keys must be %d bytes", HandshakeKeySize)
	}
	keyMaterial := encodeHandshakeFields(masterKey, sharedSecret)
	info := encodeHandshakeFields(
		[]byte("telekit/session/v3"),
		[]byte(roomID),
		[]byte(clientID),
		[]byte(serverID),
		clientNonce,
		serverNonce,
		clientEphemeralKey,
		serverEphemeralKey,
	)
	hk := hkdf.New(sha256.New, keyMaterial, salt, info)
	key := make([]byte, 32)
	if _, err := io.ReadFull(hk, key); err != nil {
		return nil, err
	}
	return key, nil
}
