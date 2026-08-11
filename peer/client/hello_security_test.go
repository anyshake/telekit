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
	currentNonce := bytes.Repeat([]byte{1}, peer.HandshakeNonceSize)
	clientPrivateKey, err := ecdh.X25519().GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	attempt := &clientHandshake{nonce: currentNonce, privateKey: clientPrivateKey, publicKey: clientPrivateKey.PublicKey().Bytes()}
	validHello := makeServerHello(t, masterKey, c.options.EncryptionType, c.options.EncryptionAAD, serverPrivateKey, serverPublicKey, c.api.RoomId, c.clientId, currentNonce, attempt.publicKey, nil)
	if _, err := c.handleServerHello(validHello, attempt); err != nil {
		t.Fatalf("valid server hello rejected: %v", err)
	}

	oldNonce := bytes.Repeat([]byte{2}, peer.HandshakeNonceSize)
	replayedHello := makeServerHello(t, masterKey, c.options.EncryptionType, c.options.EncryptionAAD, serverPrivateKey, serverPublicKey, c.api.RoomId, c.clientId, oldNonce, attempt.publicKey, nil)
	if _, err := c.handleServerHello(replayedHello, attempt); err == nil {
		t.Fatal("server hello from an older client nonce was accepted")
	}

	forgedHello := makeServerHello(t, masterKey, c.options.EncryptionType, c.options.EncryptionAAD, roguePrivateKey, serverPublicKey, c.api.RoomId, c.clientId, currentNonce, attempt.publicKey, nil)
	if _, err := c.handleServerHello(forgedHello, attempt); err == nil {
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
	if _, err := c.handleServerHello(validHello, attackerAttempt); err == nil {
		t.Fatal("PSK holder adopted a server hello bound to another client's ephemeral key")
	}

	mismatchedHeaderNonce := makeServerHello(
		t, masterKey, c.options.EncryptionType, c.options.EncryptionAAD,
		serverPrivateKey, serverPublicKey, c.api.RoomId, c.clientId,
		currentNonce, attempt.publicKey, bytes.Repeat([]byte{5}, peer.HandshakeNonceSize),
	)
	if _, err := c.handleServerHello(mismatchedHeaderNonce, attempt); err == nil {
		t.Fatal("server hello accepted a clear nonce that differed from its authenticated payload")
	}
}

func makeServerHello(
	t *testing.T,
	masterKey []byte,
	encryptionType string,
	aad []byte,
	privateKey ed25519.PrivateKey,
	pinnedPublicKey ed25519.PublicKey,
	roomID, clientID string,
	clientNonce []byte,
	clientEphemeralKey []byte,
	headerNonce []byte,
) []byte {
	t.Helper()
	serverID, err := peer.ServerIDFromPublicKey(pinnedPublicKey)
	if err != nil {
		t.Fatal(err)
	}
	serverNonce := bytes.Repeat([]byte{3}, peer.HandshakeNonceSize)
	if headerNonce == nil {
		headerNonce = serverNonce
	}
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
	handshakeKey, err := peer.DeriveServerHelloKey(masterKey, roomID, clientID, serverID, clientNonce, headerNonce)
	if err != nil {
		t.Fatal(err)
	}
	handshakeCodec, err := peer.NewCodec(encryptionType, handshakeKey, aad)
	if err != nil {
		t.Fatal(err)
	}
	data, err := handshakeCodec.EncodeMessage(&peer.Message{
		Header: &peer.Header{
			Type:           peer.MessageTypeServerHello,
			SourceId:       serverID,
			TargetId:       clientID,
			HandshakeNonce: headerNonce,
		},
		Payload: &peer.Payload{
			SessionSalt:        salt,
			ClientNonce:        clientNonce,
			ServerNonce:        serverNonce,
			ClientEphemeralKey: clientEphemeralKey,
			ServerEphemeralKey: serverEphemeralKey,
			Signature:          signature,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	return data
}
