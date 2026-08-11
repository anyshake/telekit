package peer

import (
	"bytes"
	"testing"

	"github.com/anyshake/telekit/utils/encryption"
)

func TestSignalingChannelRejectsReflection(t *testing.T) {
	sessionKey := bytes.Repeat([]byte{0x52}, HandshakeKeySize)
	client, err := NewSignalingChannel(encryption.CHACHA20_POLY1305, sessionKey, []byte("test"), DataRoleClient)
	if err != nil {
		t.Fatal(err)
	}
	server, err := NewSignalingChannel(encryption.CHACHA20_POLY1305, sessionKey, []byte("test"), DataRoleServer)
	if err != nil {
		t.Fatal(err)
	}
	message := &Message{
		Header:  &Header{Type: MessageTypeICEOffer, SourceId: "client", TargetId: "server", Sequence: 1},
		Payload: &Payload{ICEUsername: "user"},
	}
	wire, err := client.EncodeMessage(message)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := server.DecodeMessage(wire); err != nil {
		t.Fatalf("server rejected client signaling: %v", err)
	}
	if _, err := client.DecodeMessage(wire); err == nil {
		t.Fatal("client accepted its reflected signaling message")
	}
	if !bytes.Equal(client.SessionKey(), sessionKey) {
		t.Fatal("signaling channel did not retain the session transport key")
	}
}
