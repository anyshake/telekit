package client

import (
	"errors"
	"github.com/anyshake/telekit/peer"
	"github.com/anyshake/telekit/signaling"
	"github.com/pion/webrtc/v4"
)

func (c *Client) handleICECandidate(msg *peer.Message) error {
	c.signalMu.Lock()
	defer c.signalMu.Unlock()

	if msg.Payload.ICE == nil {
		return nil
	}

	if !c.remoteSet {
		size := peer.ICECandidateSize(msg.Payload.ICE)
		if len(c.pendingICE) >= c.options.MaxPendingICE || size > c.options.MaxPendingICEBytes-c.pendingICEBytes {
			return errors.New("pending ICE candidate limit reached")
		}
		c.pendingICE = append(c.pendingICE, *msg.Payload.ICE)
		c.pendingICEBytes += size
		return nil
	}

	pc := c.peerConnection()
	if pc == nil {
		return nil
	}
	return pc.AddICECandidate(*msg.Payload.ICE)
}

func (c *Client) sendICECandidate(n webrtc.ICECandidateInit) error {
	msg, err := c.codec.EncodeMessage(&peer.Message{
		Header: &peer.Header{
			SourceId: c.clientId,
			TargetId: c.serverId,
			Type:     peer.MessageTypeICE,
			Sequence: c.signalSend.Add(1),
		},
		Payload: &peer.Payload{
			ICE: &n,
		},
	})
	if err != nil {
		return err
	}

	if err = c.api.SignalingServer.Publish(c.api.RoomId, signaling.MessageOffer, msg); err != nil {
		return err
	}

	if c.options.OnICECandidateSent != nil {
		c.options.OnICECandidateSent(c, n)
	}

	return nil
}
