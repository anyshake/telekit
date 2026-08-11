package peer

import (
	"bytes"
	"crypto/ed25519"
	"crypto/rand"
	"testing"
)

func TestServerHelloSignatureBindsTranscript(t *testing.T) {
	publicKey, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	serverID, err := ServerIDFromPublicKey(publicKey)
	if err != nil {
		t.Fatal(err)
	}
	clientNonce := bytes.Repeat([]byte{1}, HandshakeNonceSize)
	serverNonce := bytes.Repeat([]byte{2}, HandshakeNonceSize)
	salt := bytes.Repeat([]byte{3}, HandshakeNonceSize)
	clientEphemeralKey := bytes.Repeat([]byte{4}, HandshakeKeySize)
	serverEphemeralKey := bytes.Repeat([]byte{5}, HandshakeKeySize)
	signature, err := SignServerHello(privateKey, "room", "client", serverID, clientNonce, serverNonce, clientEphemeralKey, serverEphemeralKey, salt)
	if err != nil {
		t.Fatal(err)
	}
	if !VerifyServerHello(publicKey, "room", "client", serverID, clientNonce, serverNonce, clientEphemeralKey, serverEphemeralKey, salt, signature) {
		t.Fatal("valid server hello signature was rejected")
	}

	tests := []struct {
		name        string
		room        string
		clientNonce []byte
		serverNonce []byte
		clientKey   []byte
		serverKey   []byte
		salt        []byte
	}{
		{name: "room", room: "other-room", clientNonce: clientNonce, serverNonce: serverNonce, clientKey: clientEphemeralKey, serverKey: serverEphemeralKey, salt: salt},
		{name: "client nonce", room: "room", clientNonce: bytes.Repeat([]byte{6}, HandshakeNonceSize), serverNonce: serverNonce, clientKey: clientEphemeralKey, serverKey: serverEphemeralKey, salt: salt},
		{name: "server nonce", room: "room", clientNonce: clientNonce, serverNonce: bytes.Repeat([]byte{7}, HandshakeNonceSize), clientKey: clientEphemeralKey, serverKey: serverEphemeralKey, salt: salt},
		{name: "client ephemeral key", room: "room", clientNonce: clientNonce, serverNonce: serverNonce, clientKey: bytes.Repeat([]byte{8}, HandshakeKeySize), serverKey: serverEphemeralKey, salt: salt},
		{name: "server ephemeral key", room: "room", clientNonce: clientNonce, serverNonce: serverNonce, clientKey: clientEphemeralKey, serverKey: bytes.Repeat([]byte{9}, HandshakeKeySize), salt: salt},
		{name: "session salt", room: "room", clientNonce: clientNonce, serverNonce: serverNonce, clientKey: clientEphemeralKey, serverKey: serverEphemeralKey, salt: bytes.Repeat([]byte{10}, HandshakeNonceSize)},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if VerifyServerHello(publicKey, test.room, "client", serverID, test.clientNonce, test.serverNonce, test.clientKey, test.serverKey, test.salt, signature) {
				t.Fatal("signature accepted a modified transcript")
			}
		})
	}
}

func TestSessionKeyBindsBothPeersAndNonces(t *testing.T) {
	master := bytes.Repeat([]byte{1}, MinPreSharedKeySize)
	salt := bytes.Repeat([]byte{2}, HandshakeNonceSize)
	clientNonce := bytes.Repeat([]byte{3}, HandshakeNonceSize)
	serverNonce := bytes.Repeat([]byte{4}, HandshakeNonceSize)
	sharedSecret := bytes.Repeat([]byte{5}, HandshakeKeySize)
	clientEphemeralKey := bytes.Repeat([]byte{6}, HandshakeKeySize)
	serverEphemeralKey := bytes.Repeat([]byte{7}, HandshakeKeySize)
	base, err := DeriveSessionKey(master, sharedSecret, salt, "room", "client", "server", clientNonce, serverNonce, clientEphemeralKey, serverEphemeralKey)
	if err != nil {
		t.Fatal(err)
	}
	changed, err := DeriveSessionKey(master, sharedSecret, salt, "room", "client", "other-server", clientNonce, serverNonce, clientEphemeralKey, serverEphemeralKey)
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Equal(base, changed) {
		t.Fatal("server identity did not affect the session key")
	}
	changed, err = DeriveSessionKey(master, sharedSecret, salt, "room", "client", "server", bytes.Repeat([]byte{9}, HandshakeNonceSize), serverNonce, clientEphemeralKey, serverEphemeralKey)
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Equal(base, changed) {
		t.Fatal("client nonce did not affect the session key")
	}
	changed, err = DeriveSessionKey(master, bytes.Repeat([]byte{10}, HandshakeKeySize), salt, "room", "client", "server", clientNonce, serverNonce, clientEphemeralKey, serverEphemeralKey)
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Equal(base, changed) {
		t.Fatal("X25519 shared secret did not affect the session key")
	}
}

func TestHandshakeKeysAreScopedAndDirectional(t *testing.T) {
	master := bytes.Repeat([]byte{0x41}, MinPreSharedKeySize)
	clientNonce := bytes.Repeat([]byte{0x42}, HandshakeNonceSize)
	serverNonce := bytes.Repeat([]byte{0x43}, HandshakeNonceSize)
	clientKey, err := DeriveClientHelloKey(master, "room", "client", "server", clientNonce)
	if err != nil {
		t.Fatal(err)
	}
	serverKey, err := DeriveServerHelloKey(master, "room", "client", "server", clientNonce, serverNonce)
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Equal(clientKey, serverKey) {
		t.Fatal("client and server hello directions derived the same key")
	}
	changedNonce, err := DeriveClientHelloKey(master, "room", "client", "server", bytes.Repeat([]byte{0x44}, HandshakeNonceSize))
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Equal(clientKey, changedNonce) {
		t.Fatal("client hello nonce did not scope the key")
	}
	changedIdentity, err := DeriveClientHelloKey(master, "other-room", "client", "server", clientNonce)
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Equal(clientKey, changedIdentity) {
		t.Fatal("room identity did not scope the key")
	}
}
