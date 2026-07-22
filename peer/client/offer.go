package client

import (
	"errors"

	"github.com/anyshake/telekit/peer"
	"github.com/anyshake/telekit/signaling"
)

func (c *Client) sendOffer() error {
	pc := c.peerConnection()
	if pc == nil {
		return errors.New("peer connection is closed")
	}
	offer, err := pc.CreateOffer(nil)
	if err != nil {
		return err
	}
	if err := pc.SetLocalDescription(offer); err != nil {
		return err
	}

	sdp := pc.LocalDescription()

	dataBytes, err := c.codec.EncodeMessage(&peer.Message{
		Header: &peer.Header{
			SourceId: c.clientId,
			TargetId: c.serverId,
			Type:     peer.MessageTypeOffer,
			Sequence: c.signalSend.Add(1),
		},
		Payload: &peer.Payload{
			SDP: sdp,
		},
	})
	if err != nil {
		return err
	}
	if err = c.api.SignalingServer.Publish(c.api.RoomId, signaling.MessageOffer, dataBytes); err != nil {
		return err
	}
	if c.options.OnOfferSent != nil {
		c.options.OnOfferSent(c, sdp)
	}

	return nil
}
