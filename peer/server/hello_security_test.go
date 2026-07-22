package server

import (
	"bytes"
	"testing"
	"time"

	"github.com/alphadose/haxmap"
	"github.com/anyshake/telekit/peer"
	peerapi "github.com/anyshake/telekit/peer/api"
	"github.com/anyshake/telekit/utils/encryption"
	lru "github.com/hashicorp/golang-lru/v2"
)

func TestClientProofRejectsRelabeledIdentity(t *testing.T) {
	key := bytes.Repeat([]byte{0x42}, peer.MinPreSharedKeySize)
	codec, err := peer.NewCodec(encryption.XCHACHA20_POLY1305, key, []byte("test"), false)
	if err != nil {
		t.Fatal(err)
	}
	data, err := codec.EncodeMessage(&peer.Message{
		Header: &peer.Header{
			SourceId: "attacker",
			Type:     peer.MessageTypeClientHello,
		},
		Payload: &peer.Payload{
			ClientNonce:       bytes.Repeat([]byte{1}, peer.HandshakeNonceSize),
			HandshakeRoomID:   "room",
			HandshakeClientID: "real-client",
			Timestamp:         []byte(time.Now().UTC().Format(time.RFC3339Nano)),
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	cache, err := lru.New[string, int64](16)
	if err != nil {
		t.Fatal(err)
	}
	s := &Server{
		api: &peerapi.API{RoomId: "room"},
		options: &Options{
			KeyProvider:    peer.KeyProviderFunc(func(string) ([]byte, error) { return key, nil }),
			EncryptionType: encryption.XCHACHA20_POLY1305,
			EncryptionAAD:  []byte("test"),
			ClockSkew:      time.Minute,
		},
		defaultCodec: &peer.Codec{},
		nonceCache:   cache,
		connections:  haxmap.New[string, *Connection](),
	}
	if _, err := s.handleClientHello(data); err == nil {
		t.Fatal("client hello with a relabeled identity was accepted")
	}
}

func TestPendingClientHelloStillRequiresPSKProof(t *testing.T) {
	key := bytes.Repeat([]byte{0x42}, peer.MinPreSharedKeySize)
	wrongKey := bytes.Repeat([]byte{0x24}, peer.MinPreSharedKeySize)
	wrongCodec, err := peer.NewCodec(encryption.XCHACHA20_POLY1305, wrongKey, []byte("test"), false)
	if err != nil {
		t.Fatal(err)
	}
	data, err := wrongCodec.EncodeMessage(&peer.Message{
		Header: &peer.Header{SourceId: "client", Type: peer.MessageTypeClientHello},
		Payload: &peer.Payload{
			ClientNonce:        bytes.Repeat([]byte{1}, peer.HandshakeNonceSize),
			ClientEphemeralKey: bytes.Repeat([]byte{2}, peer.HandshakeKeySize),
			HandshakeRoomID:    "room",
			HandshakeClientID:  "client",
			Timestamp:          []byte(time.Now().UTC().Format(time.RFC3339Nano)),
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	cache, err := lru.New[string, int64](16)
	if err != nil {
		t.Fatal(err)
	}
	connections := haxmap.New[string, *Connection]()
	connections.Set("client", &Connection{
		sourceId:           "client",
		clientNonce:        bytes.Repeat([]byte{1}, peer.HandshakeNonceSize),
		clientEphemeralKey: bytes.Repeat([]byte{2}, peer.HandshakeKeySize),
	})
	s := &Server{
		api: &peerapi.API{RoomId: "room"},
		options: &Options{
			KeyProvider:    peer.KeyProviderFunc(func(string) ([]byte, error) { return key, nil }),
			EncryptionType: encryption.XCHACHA20_POLY1305,
			EncryptionAAD:  []byte("test"),
			ClockSkew:      time.Minute,
		},
		defaultCodec: &peer.Codec{},
		nonceCache:   cache,
		connections:  connections,
	}
	if conn, err := s.handleClientHello(data); err == nil || conn != nil {
		t.Fatal("an unauthenticated hello reused an existing pending client session")
	}
}
