package server

import (
	"errors"
	"fmt"
	"net"
	"sync"

	"github.com/anyshake/telekit/peer"
	"github.com/anyshake/telekit/signaling"
	"github.com/pion/webrtc/v4"
)

var roomOwners sync.Map

func (s *Server) Listen() error {
	select {
	case <-s.closeCh:
		return net.ErrClosed
	default:
	}
	if s.onHandshake != nil || s.onOffer != nil {
		return errors.New("server is already listening")
	}
	leaseKey := fmt.Sprintf("%T:%p:%s", s.api.SignalingServer, s.api.SignalingServer, s.api.RoomId)
	if identifiable, ok := s.api.SignalingServer.(signaling.Identifiable); ok {
		leaseKey = identifiable.SignalingID() + ":" + s.api.RoomId
	}
	if _, loaded := roomOwners.LoadOrStore(leaseKey, s); loaded {
		return fmt.Errorf("room %q already has a server", s.api.RoomId)
	}
	s.roomLeaseKey = leaseKey
	succeeded := false
	defer func() {
		if !succeeded {
			if s.onHandshake != nil {
				_ = s.onHandshake.Unsubscribe()
				s.onHandshake = nil
			}
			roomOwners.Delete(leaseKey)
			s.roomLeaseKey = ""
		}
	}()

	var err error

	s.onHandshake, err = s.api.SignalingServer.Subscribe(s.api.RoomId, signaling.MessageHello, func(data []byte) {
		if !s.helloLimiter.Allow() {
			return
		}
		conn, err := s.handleClientHello(data)
		if conn == nil || err != nil {
			return
		}
		// Publish only after installing the session. Fast transports may deliver
		// the client's offer before Publish returns.
		s.connections.Set(conn.sourceId, conn)
		conn.startHandshakeTimer(s.options.HandshakeTimeout)
		if err := s.sendServerHello(conn.handshakeCodec, conn.sourceId, conn.sessionSalt); err != nil {
			s.connections.Del(conn.sourceId)
			_ = conn.Close()
			return
		}
	})
	if err != nil {
		return err
	}

	s.onOffer, err = s.api.SignalingServer.Subscribe(s.api.RoomId, signaling.MessageOffer, func(data []byte) {
		header, err := s.defaultCodec.DecodeMessageHeader(data)
		if err != nil {
			return
		}
		if header.TargetId != s.serverId {
			return
		}
		conn, ok := s.connections.Get(header.SourceId)
		if !ok {
			return
		}
		fullMsg, err := conn.codec.DecodeMessage(data)
		if err != nil {
			return
		}
		if !conn.signalRecv.Accept(fullMsg.Header.Sequence) {
			return
		}
		switch fullMsg.Header.Type {
		case peer.MessageTypeOffer:
			if fullMsg.Payload.SDP == nil || fullMsg.Payload.SDP.Type != webrtc.SDPTypeOffer {
				return
			}
			if conn.peerConnection() != nil {
				if err = s.handleOffer(conn, fullMsg); err != nil {
					return
				}
				if s.options.OnClientOffer != nil {
					s.options.OnClientOffer(conn, fullMsg.Payload.SDP)
				}
			}
		case peer.MessageTypeICE:
			if fullMsg.Payload.ICE == nil {
				return
			}
			_ = s.handleICECandidate(conn, fullMsg)
		}
	})
	if err != nil {
		return err
	}

	succeeded = true
	return nil
}
