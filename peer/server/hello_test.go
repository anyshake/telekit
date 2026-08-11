package server

import (
	"bytes"
	"errors"
	"testing"
	"time"

	"github.com/anyshake/telekit/peer"
	lru "github.com/hashicorp/golang-lru/v2"
)

func TestPrepareClientHelloRejectsReplayBeforeClosingExisting(t *testing.T) {
	now := time.Now()
	nonceCache, err := lru.New[string, int64](16)
	if err != nil {
		t.Fatal(err)
	}
	s := &Server{
		nonceCache: nonceCache,
		options: &Options{
			GetTimeFunc:      func() time.Time { return now },
			ReplayProtection: time.Minute,
		},
	}

	nonce := bytes.Repeat([]byte{1}, peer.HandshakeNonceSize)
	if !s.isNonceAvailable(nonce) {
		t.Fatal("failed to reserve test nonce")
	}
	existing := &Connection{
		recvBuf: peer.NewRecvBufferWithLimit(1024, nil),
	}

	reuse, err := s.prepareClientHello(existing, nonce, bytes.Repeat([]byte{2}, peer.HandshakeKeySize))
	if !errors.Is(err, errReplayedClientHello) {
		t.Fatalf("prepareClientHello error = %v, want replay error", err)
	}
	if reuse {
		t.Fatal("replayed hello was treated as a pending-session retry")
	}
	if existing.recvBuf.IsClosed() {
		t.Fatal("replayed hello closed the existing session")
	}
}
