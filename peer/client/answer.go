package client

import (
	"errors"

	"github.com/anyshake/telekit/peer"
	"github.com/pion/webrtc/v4"
)

func (c *Client) handleAnswer(msg *peer.Message) error {
	c.signalMu.Lock()
	defer c.signalMu.Unlock()

	if msg == nil || msg.Payload == nil || msg.Payload.SDP == nil || msg.Payload.SDP.Type != webrtc.SDPTypeAnswer {
		return errors.New("answer SDP is missing or has the wrong type")
	}

	pc := c.peerConnection()
	if pc == nil {
		return errors.New("peer connection is closed")
	}
	if err := pc.SetRemoteDescription(*msg.Payload.SDP); err != nil {
		return err
	}

	c.remoteSet = true

	for _, ice := range c.pendingICE {
		_ = pc.AddICECandidate(ice)
	}
	c.pendingICE = nil
	c.pendingICEBytes = 0

	return nil
}
