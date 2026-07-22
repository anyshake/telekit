package client

import (
	"testing"

	"github.com/anyshake/telekit/peer"
	"github.com/pion/webrtc/v4"
)

func TestPendingICELimits(t *testing.T) {
	c := &Client{options: &Options{MaxPendingICE: 1, MaxPendingICEBytes: 64}}
	msg := &peer.Message{Payload: &peer.Payload{ICE: &webrtc.ICECandidateInit{Candidate: "candidate"}}}
	if err := c.handleICECandidate(msg); err != nil {
		t.Fatal(err)
	}
	if err := c.handleICECandidate(msg); err == nil {
		t.Fatal("pending ICE count limit was not enforced")
	}
}
