package server

import (
	"errors"

	"github.com/anyshake/telekit/peer"
	"github.com/anyshake/telekit/signaling"
)

func (s *Server) sendAnswer(conn *Connection, msg *peer.Message) error {
	pc := conn.peerConnection()
	if pc == nil {
		return errors.New("peer connection is closed")
	}
	answer, err := pc.CreateAnswer(nil)
	if err != nil {
		return err
	}
	if err := pc.SetLocalDescription(answer); err != nil {
		return err
	}

	sdp := pc.LocalDescription()

	dataBytes, err := conn.codec.EncodeMessage(&peer.Message{
		Header: &peer.Header{
			SourceId: s.serverId,
			TargetId: msg.Header.SourceId,
			Type:     peer.MessageTypeAnswer,
			Sequence: conn.signalSend.Add(1),
		},
		Payload: &peer.Payload{
			SDP: sdp,
		},
		Encrypt: true,
	})
	if err != nil {
		return err
	}

	if err = s.api.SignalingServer.Publish(s.api.RoomId, signaling.MessageAnswer, dataBytes); err != nil {
		return err
	}

	if s.options.OnAnswerSent != nil {
		s.options.OnAnswerSent(conn, sdp)
	}

	return err
}
