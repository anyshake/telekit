package client

import (
	"bytes"
	"crypto/ecdh"
	"crypto/ed25519"
	"crypto/rand"
	"testing"

	"github.com/anyshake/telekit/peer"
	peerapi "github.com/anyshake/telekit/peer/api"
	"github.com/anyshake/telekit/utils/encryption"
)

func TestServerHelloRejectsReplayAndForgedServer(t *testing.T) {
	serverPublicKey, serverPrivateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	_, roguePrivateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	masterKey := bytes.Repeat([]byte{0x42}, peer.MinPreSharedKeySize)
	c := &Client{
		clientId: "sensor-01",
		psk: peer.PreSharedKey{
			ClientID:        "sensor-01",
			Key:             masterKey,
			ServerPublicKey: serverPublicKey,
		},
		api: &peerapi.API{RoomId: "room"},
		options: &Options{
			EncryptionType: encryption.XCHACHA20_POLY1305,
			EncryptionAAD:  []byte("test"),
		},
	}
	handshakeCodec, err := peer.NewCodec(encryption.XCHACHA20_POLY1305, masterKey, c.options.EncryptionAAD, false)
	if err != nil {
		t.Fatal(err)
	}
	currentNonce := bytes.Repeat([]byte{1}, peer.HandshakeNonceSize)
	clientPrivateKey, err := ecdh.X25519().GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	attempt := &clientHandshake{nonce: currentNonce, privateKey: clientPrivateKey, publicKey: clientPrivateKey.PublicKey().Bytes()}
	validHello := makeServerHello(t, handshakeCodec, serverPrivateKey, serverPublicKey, c.api.RoomId, c.clientId, currentNonce, attempt.publicKey)
	if _, err := c.handleServerHello(handshakeCodec, validHello, attempt); err != nil {
		t.Fatalf("valid server hello rejected: %v", err)
	}

	oldNonce := bytes.Repeat([]byte{2}, peer.HandshakeNonceSize)
	replayedHello := makeServerHello(t, handshakeCodec, serverPrivateKey, serverPublicKey, c.api.RoomId, c.clientId, oldNonce, attempt.publicKey)
	if _, err := c.handleServerHello(handshakeCodec, replayedHello, attempt); err == nil {
		t.Fatal("server hello from an older client nonce was accepted")
	}

	forgedHello := makeServerHello(t, handshakeCodec, roguePrivateKey, serverPublicKey, c.api.RoomId, c.clientId, currentNonce, attempt.publicKey)
	if _, err := c.handleServerHello(handshakeCodec, forgedHello, attempt); err == nil {
		t.Fatal("server hello signed by an unpinned key was accepted")
	}

	attackerPrivateKey, err := ecdh.X25519().GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	attackerAttempt := &clientHandshake{
		nonce:      currentNonce,
		privateKey: attackerPrivateKey,
		publicKey:  attackerPrivateKey.PublicKey().Bytes(),
	}
	if _, err := c.handleServerHello(handshakeCodec, validHello, attackerAttempt); err == nil {
		t.Fatal("PSK holder adopted a server hello bound to another client's ephemeral key")
	}
}

func makeServerHello(
	t *testing.T,
	handshakeCodec *peer.Codec,
	privateKey ed25519.PrivateKey,
	pinnedPublicKey ed25519.PublicKey,
	roomID, clientID string,
	clientNonce []byte,
	clientEphemeralKey []byte,
) []byte {
	t.Helper()
	serverID, err := peer.ServerIDFromPublicKey(pinnedPublicKey)
	if err != nil {
		t.Fatal(err)
	}
	serverNonce := bytes.Repeat([]byte{3}, peer.HandshakeNonceSize)
	salt := bytes.Repeat([]byte{4}, peer.HandshakeNonceSize)
	serverPrivateKey, err := ecdh.X25519().GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	serverEphemeralKey := serverPrivateKey.PublicKey().Bytes()
	signature, err := peer.SignServerHello(privateKey, roomID, clientID, serverID, clientNonce, serverNonce, clientEphemeralKey, serverEphemeralKey, salt)
	if err != nil {
		t.Fatal(err)
	}
	data, err := handshakeCodec.EncodeMessage(&peer.Message{
		Header: &peer.Header{
			Type:     peer.MessageTypeServerHello,
			SourceId: serverID,
			TargetId: clientID,
		},
		Payload: &peer.Payload{
			SessionSalt:        salt,
			ClientNonce:        clientNonce,
			ServerNonce:        serverNonce,
			ClientEphemeralKey: clientEphemeralKey,
			ServerEphemeralKey: serverEphemeralKey,
			Signature:          signature,
		},
		Encrypt: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	return data
}
