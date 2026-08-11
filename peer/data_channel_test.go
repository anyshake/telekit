package peer

import (
	"bytes"
	"errors"
	"testing"

	"github.com/anyshake/telekit/utils/encryption"
)

func TestDataChannelRejectsReplayAndReflection(t *testing.T) {
	sessionKey := bytes.Repeat([]byte{0x42}, 32)
	client, err := NewDataChannel(encryption.CHACHA20_POLY1305, sessionKey, []byte("test"), DataRoleClient)
	if err != nil {
		t.Fatal(err)
	}
	server, err := NewDataChannel(encryption.CHACHA20_POLY1305, sessionKey, []byte("test"), DataRoleServer)
	if err != nil {
		t.Fatal(err)
	}

	sequence, ciphertext, err := client.Seal([]byte("request"))
	if err != nil {
		t.Fatal(err)
	}
	plaintext, err := server.Open(sequence, ciphertext, 1024)
	if err != nil || string(plaintext) != "request" {
		t.Fatalf("server Open = %q, %v", plaintext, err)
	}
	if _, err := server.Open(sequence, ciphertext, 1024); !errors.Is(err, ErrDataFrameReplay) {
		t.Fatalf("replayed frame error = %v", err)
	}
	if _, err := client.Open(sequence, ciphertext, 1024); err == nil {
		t.Fatal("client accepted its reflected outbound frame")
	}
	nextSequence, nextCiphertext, err := client.Seal([]byte("next request"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := server.Open(1000, nextCiphertext, 1024); err == nil {
		t.Fatal("server accepted ciphertext with a forged high sequence")
	}
	plaintext, err = server.Open(nextSequence, nextCiphertext, 1024)
	if err != nil || string(plaintext) != "next request" {
		t.Fatalf("server Open after forged sequence = %q, %v", plaintext, err)
	}

	sequence, ciphertext, err = server.Seal([]byte("response"))
	if err != nil {
		t.Fatal(err)
	}
	plaintext, err = client.Open(sequence, ciphertext, 1024)
	if err != nil || string(plaintext) != "response" {
		t.Fatalf("client Open = %q, %v", plaintext, err)
	}
}

func TestDataChannelAuthenticatesHeartbeat(t *testing.T) {
	sessionKey := bytes.Repeat([]byte{0x24}, 32)
	client, err := NewDataChannel(encryption.CHACHA20_POLY1305, sessionKey, []byte("test"), DataRoleClient)
	if err != nil {
		t.Fatal(err)
	}
	server, err := NewDataChannel(encryption.CHACHA20_POLY1305, sessionKey, []byte("test"), DataRoleServer)
	if err != nil {
		t.Fatal(err)
	}

	sequence, ciphertext, err := client.SealHeartbeat()
	if err != nil {
		t.Fatal(err)
	}
	frameType, payload, err := server.OpenFrame(sequence, ciphertext, 1024)
	if err != nil {
		t.Fatal(err)
	}
	if frameType != DataFrameHeartbeat || len(payload) != 0 {
		t.Fatalf("heartbeat frame = %d, %x", frameType, payload)
	}
	if _, _, err := server.OpenFrame(sequence, ciphertext, 1024); !errors.Is(err, ErrDataFrameReplay) {
		t.Fatalf("replayed heartbeat error = %v", err)
	}
	if _, err := server.Open(sequence+1, ciphertext, 1024); err == nil {
		t.Fatal("heartbeat accepted as application data")
	}
}
