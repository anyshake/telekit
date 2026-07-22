package peer

import (
	"bytes"
	"crypto/ed25519"
	"strings"
	"testing"

	"github.com/anyshake/telekit/utils/encryption"
)

func TestDeriveSessionKeyIsScoped(t *testing.T) {
	master := bytes.Repeat([]byte{1}, MinPreSharedKeySize)
	salt := bytes.Repeat([]byte{2}, HandshakeNonceSize)
	clientNonce := bytes.Repeat([]byte{3}, HandshakeNonceSize)
	serverNonce := bytes.Repeat([]byte{4}, HandshakeNonceSize)
	sharedSecret := bytes.Repeat([]byte{5}, HandshakeKeySize)
	clientEphemeralKey := bytes.Repeat([]byte{6}, HandshakeKeySize)
	serverEphemeralKey := bytes.Repeat([]byte{7}, HandshakeKeySize)
	a, err := DeriveSessionKey(master, sharedSecret, salt, "room-a", "client", "server", clientNonce, serverNonce, clientEphemeralKey, serverEphemeralKey)
	if err != nil {
		t.Fatal(err)
	}
	b, err := DeriveSessionKey(master, sharedSecret, salt, "room-b", "client", "server", clientNonce, serverNonce, clientEphemeralKey, serverEphemeralKey)
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Equal(a, b) {
		t.Fatal("session keys for different rooms must differ")
	}
}

func TestCodecSessionUpdateDoesNotOverwritePSK(t *testing.T) {
	master := bytes.Repeat([]byte{1}, MinPreSharedKeySize)
	want := append([]byte(nil), master...)
	codec, err := NewCodec(encryption.XCHACHA20_POLY1305, master, []byte("test"), false)
	if err != nil {
		t.Fatal(err)
	}
	if err := codec.UpdateSecret(encryption.XCHACHA20_POLY1305, bytes.Repeat([]byte{2}, 32)); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(master, want) {
		t.Fatal("updating the session key overwrote caller-owned PSK memory")
	}
}

func TestPreSharedKeyValidation(t *testing.T) {
	publicKey := ed25519.NewKeyFromSeed(make([]byte, ed25519.SeedSize)).Public().(ed25519.PublicKey)
	if err := (PreSharedKey{ClientID: "client", Key: make([]byte, MinPreSharedKeySize), ServerPublicKey: publicKey}).Validate(); err != nil {
		t.Fatal(err)
	}
	if err := (PreSharedKey{ClientID: "client", Key: []byte("short"), ServerPublicKey: publicKey}).Validate(); err == nil {
		t.Fatal("short key was accepted")
	}
	if err := (PreSharedKey{ClientID: "client", Key: make([]byte, MinPreSharedKeySize)}).Validate(); err == nil {
		t.Fatal("missing server public key was accepted")
	}
	if err := ValidateClientID(strings.Repeat("a", MaxClientIDLength+1)); err == nil {
		t.Fatal("oversized client ID was accepted")
	}
}
