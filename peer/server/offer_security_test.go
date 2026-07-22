package server

import (
	"testing"

	"github.com/anyshake/telekit/peer"
	"github.com/pion/webrtc/v4"
)

func TestHandleOfferRejectsMissingOrWrongSDP(t *testing.T) {
	s := &Server{}
	conn := &Connection{}
	for _, sdp := range []*webrtc.SessionDescription{
		nil,
		{Type: webrtc.SDPTypeAnswer},
	} {
		msg := &peer.Message{Payload: &peer.Payload{SDP: sdp}}
		if err := s.handleOffer(conn, msg); err == nil {
			t.Fatal("invalid offer payload was accepted")
		}
	}
}
