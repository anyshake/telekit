package server

import (
	"testing"

	"github.com/anyshake/telekit/peer"
	"github.com/pion/webrtc/v4"
)

func TestPendingICEUsesGlobalBudget(t *testing.T) {
	s := &Server{
		options:      &Options{MaxPendingICE: 2, MaxPendingICEBytes: 64},
		bufferBudget: peer.NewByteBudget(32),
	}
	conn := &Connection{owner: s}
	msg := &peer.Message{Payload: &peer.Payload{ICE: &webrtc.ICECandidateInit{Candidate: "candidate"}}}
	if err := s.handleICECandidate(conn, msg); err != nil {
		t.Fatal(err)
	}
	if s.bufferBudget.Used() == 0 {
		t.Fatal("pending ICE did not reserve global memory")
	}
	if err := s.handleICECandidate(conn, msg); err == nil {
		t.Fatal("global ICE memory budget was not enforced")
	}
	conn.resetPendingICE()
	if used := s.bufferBudget.Used(); used != 0 {
		t.Fatalf("budget used after reset = %d", used)
	}
}
