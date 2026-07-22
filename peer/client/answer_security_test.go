package client

import (
	"testing"

	"github.com/anyshake/telekit/peer"
	"github.com/pion/webrtc/v4"
)

func TestHandleAnswerRejectsMissingOrWrongSDP(t *testing.T) {
	c := &Client{}
	for _, sdp := range []*webrtc.SessionDescription{
		nil,
		{Type: webrtc.SDPTypeOffer},
	} {
		msg := &peer.Message{Payload: &peer.Payload{SDP: sdp}}
		if err := c.handleAnswer(msg); err == nil {
			t.Fatal("invalid answer payload was accepted")
		}
	}
}
