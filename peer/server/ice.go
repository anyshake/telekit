package server

import (
	"errors"
	"github.com/anyshake/telekit/peer"
	"github.com/anyshake/telekit/signaling"
	"github.com/pion/webrtc/v4"
)

func (s *Server) handleICECandidate(conn *Connection, msg *peer.Message) error {
	conn.signalMu.Lock()
	defer conn.signalMu.Unlock()

	if msg.Payload.ICE == nil {
		return nil
	}

	if !conn.remoteSet {
		size := peer.ICECandidateSize(msg.Payload.ICE)
		if len(conn.pendingICE) >= s.options.MaxPendingICE ||
			size > s.options.MaxPendingICEBytes-conn.pendingICEBytes || !s.bufferBudget.Reserve(size) {
			return errors.New("pending ICE candidate limit reached")
		}
		conn.pendingICE = append(conn.pendingICE, *msg.Payload.ICE)
		conn.pendingICEBytes += size
		return nil
	}

	pc := conn.peerConnection()
	if pc == nil {
		return nil
	}
	return pc.AddICECandidate(*msg.Payload.ICE)
}

func (s *Server) sendICECandidate(conn *Connection, n webrtc.ICECandidateInit) error {
	dataBytes, err := conn.codec.EncodeMessage(&peer.Message{
		Header: &peer.Header{
			SourceId: s.serverId,
			TargetId: conn.sourceId,
			Type:     peer.MessageTypeICE,
			Sequence: conn.signalSend.Add(1),
		},
		Payload: &peer.Payload{
			ICE: &n,
		},
		Encrypt: true,
	})
	if err != nil {
		return err
	}

	if err = s.api.SignalingServer.Publish(s.api.RoomId, signaling.MessageAnswer, dataBytes); err != nil {
		return err
	}

	if s.options.OnICECandidateSent != nil {
		s.options.OnICECandidateSent(conn, n)
	}

	return nil
}
