package server

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/anyshake/telekit/peer"
	"github.com/anyshake/telekit/relays"
	"github.com/anyshake/telekit/signaling"
	transportcore "github.com/anyshake/telekit/transports"
	"github.com/pion/ice/v4"
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
			if s.onOffer != nil {
				_ = s.onOffer.Unsubscribe()
				s.onOffer = nil
			}
			roomOwners.Delete(leaseKey)
			s.roomLeaseKey = ""
		}
	}()

	var err error
	s.onHandshake, err = s.api.SignalingServer.Subscribe(s.api.RoomId, signaling.MessageHello, func(data []byte) {
		s.helloMu.Lock()
		defer s.helloMu.Unlock()
		if !s.helloLimiter.Allow() {
			return
		}
		conn, err := s.handleClientHello(data)
		if conn == nil || err != nil {
			return
		}
		if !s.setConnection(conn) {
			_ = conn.Close()
			return
		}
		conn.startHandshakeTimer(s.options.HandshakeTimeout)
		if err := s.sendServerHello(conn); err != nil {
			_ = conn.Close()
		}
	})
	if err != nil {
		return err
	}

	s.onOffer, err = s.api.SignalingServer.Subscribe(s.api.RoomId, signaling.MessageOffer, func(data []byte) {
		header, err := s.defaultCodec.DecodeMessageHeader(data)
		if err != nil || header.TargetId != s.serverId {
			return
		}
		conn, ok := s.connections.Get(header.SourceId)
		if !ok {
			return
		}
		msg, err := conn.codec.DecodeMessage(data)
		if err != nil || !conn.signalRecv.Accept(msg.Header.Sequence) || msg.Payload == nil {
			return
		}
		if !conn.isCurrent() {
			return
		}
		switch msg.Header.Type {
		case peer.MessageTypeDisconnect:
			// The client sends this before closing its data transport. Removing
			// the authenticated session here makes reconnect independent of
			// transport-specific close propagation.
			_ = conn.Close()
		case peer.MessageTypeTransportSelect:
			if !containsTransport(s.options.Transports, msg.Payload.Transport) {
				return
			}
			var pendingOffer *transportcore.ICEDescription
			conn.stateMu.Lock()
			if !conn.closed.Load() && conn.selectedTransport == "" {
				conn.selectedTransport = msg.Payload.Transport
			}
			if !conn.closed.Load() && conn.pendingICEOffer != nil {
				pendingOffer = conn.pendingICEOffer
				conn.pendingICEOffer = nil
			}
			conn.stateMu.Unlock()
			if pendingOffer != nil {
				go s.handleICEOffer(conn, *pendingOffer)
			}
		case peer.MessageTypeICEOffer:
			if msg.Payload.ICEUsername == "" || msg.Payload.ICEPassword == "" || len(msg.Payload.ICECandidates) == 0 {
				return
			}
			offer := transportcore.ICEDescription{
				UsernameFragment: msg.Payload.ICEUsername,
				Password:         msg.Payload.ICEPassword,
				Candidates:       msg.Payload.ICECandidates,
			}
			if err := transportcore.ValidateICEDescriptionLimits(offer, s.options.MaxPendingICE, s.options.MaxPendingICEBytes); err != nil {
				return
			}
			offer.Candidates = append([]string(nil), offer.Candidates...)
			if s.options.OnICEOffer != nil && conn.isCurrent() {
				s.options.OnICEOffer(conn, offer)
			}
			conn.stateMu.Lock()
			selected := !conn.closed.Load() && conn.selectedTransport != ""
			if !conn.closed.Load() && !selected {
				conn.pendingICEOffer = &offer
			}
			conn.stateMu.Unlock()
			if selected {
				go s.handleICEOffer(conn, offer)
			}
		}
	})
	if err != nil {
		return err
	}
	succeeded = true
	return nil
}

func (s *Server) handleICEOffer(conn *Connection, remote transportcore.ICEDescription) {
	selectedName, ok := conn.selectedTransportForSetup()
	if !ok {
		return
	}
	agentOptions := append([]ice.AgentOption(nil), s.options.ICEAgentOptions...)
	agent, err := transportcore.NewICEAgent(s.api.ICEURLs, agentOptions...)
	if err != nil {
		_ = conn.Close()
		return
	}
	if !conn.installICEAgent(agent) {
		return
	}
	relayProvider, err := s.api.WebSocketRelayProvider(s.serverId, conn.sourceId)
	if err != nil {
		_ = conn.Close()
		return
	}
	description, err := transportcore.GatherICEWithCallback(agent, func(candidate ice.Candidate) {
		if !conn.ownsICEAgent(agent) {
			return
		}
		if candidate == nil {
			if s.options.OnICECandidateGatheringComplete != nil {
				s.options.OnICECandidateGatheringComplete(conn)
			}
			return
		}
		if s.options.OnICECandidate != nil {
			s.options.OnICECandidate(conn, candidate)
		}
	}, func() error {
		if !conn.ownsICEAgent(agent) {
			return net.ErrClosed
		}
		if relayProvider == nil {
			return nil
		}
		ctx, cancel := context.WithTimeout(context.Background(), s.options.HandshakeTimeout)
		defer cancel()
		ufrag, _, err := agent.GetLocalUserCredentials()
		if err != nil {
			return err
		}
		candidate, packetConn, err := relayProvider.AllocateCandidate(ctx, ufrag)
		if err != nil {
			return err
		}
		if err := relays.AddLocalCandidate(agent, candidate, packetConn); err != nil {
			_ = packetConn.Close()
			return err
		}
		return nil
	})
	if err != nil {
		if conn.ownsICEAgent(agent) {
			_ = conn.Close()
		} else {
			_ = agent.Close()
		}
		return
	}
	if !conn.ownsICEAgent(agent) {
		_ = agent.Close()
		return
	}
	if err := transportcore.ValidateICEDescriptionLimits(description, s.options.MaxPendingICE, s.options.MaxPendingICEBytes); err != nil {
		_ = conn.Close()
		return
	}
	if s.options.OnICEAnswer != nil {
		s.options.OnICEAnswer(conn, description)
	}
	if !conn.ownsICEAgent(agent) {
		return
	}
	if err := s.sendICEAnswer(conn, description); err != nil {
		_ = conn.Close()
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), s.options.HandshakeTimeout)
	defer cancel()
	iceConn, err := transportcore.AcceptICE(ctx, agent, remote)
	if err != nil {
		_ = conn.Close()
		return
	}
	if !conn.ownsICEAgent(agent) {
		_ = iceConn.Close()
		return
	}
	selected := findTransport(s.options.Transports, selectedName)
	if selected == nil {
		_ = iceConn.Close()
		_ = conn.Close()
		return
	}
	endpointLocal := peer.Addr{RoomID: conn.roomId, PeerID: conn.serverId}
	endpointRemote := peer.Addr{RoomID: conn.roomId, PeerID: conn.sourceId}
	transportKey := conn.codec.SessionKey()
	endpoint := transportcore.ICEEndpoint(iceConn, endpointLocal, endpointRemote, transportKey)
	dataConn, err := selected.Accept(ctx, endpoint)
	if err != nil {
		_ = iceConn.Close()
		_ = conn.Close()
		return
	}
	if !conn.installTransport(agent, dataConn, transportMaxFrameSize(selected), iceConn.LocalAddr(), iceConn.RemoteAddr()) {
		_ = iceConn.Close()
		return
	}
	conn.lastTransportRead.Store(s.options.GetTimeFunc().UnixNano())
	_, dataChannel := conn.transportState()
	if !conn.markEstablished(agent, dataConn) {
		_ = conn.Close()
		return
	}
	go conn.readTransport(dataConn, dataChannel, transportPacketMode(selected))
	go monitorTransport(conn, dataConn, dataChannel)
	if !conn.isCurrentTransport(dataConn, dataChannel) {
		_ = conn.Close()
		return
	}
	select {
	case s.acceptCh <- conn:
		if s.options.OnConnected != nil && conn.isCurrentTransport(dataConn, dataChannel) {
			s.options.OnConnected(conn)
		}
	case <-s.closeCh:
		_ = conn.Close()
	default:
		_ = conn.Close()
	}
}

func monitorTransport(conn *Connection, transportConn net.Conn, dataChannel *peer.DataChannel) {
	ticker := time.NewTicker(conn.owner.options.HeartbeatInterval)
	defer ticker.Stop()
	for range ticker.C {
		if !conn.isCurrentTransport(transportConn, dataChannel) {
			return
		}
		lastRead := time.Unix(0, conn.lastTransportRead.Load())
		if conn.owner.options.GetTimeFunc().Sub(lastRead) >= conn.owner.options.HeartbeatTimeout {
			_ = conn.Close()
			return
		}
		if err := conn.sendHeartbeat(transportConn, dataChannel); err != nil {
			_ = conn.Close()
			return
		}
	}
}

func (s *Server) sendICEAnswer(conn *Connection, description transportcore.ICEDescription) error {
	if err := transportcore.ValidateICEDescriptionLimits(description, s.options.MaxPendingICE, s.options.MaxPendingICEBytes); err != nil {
		return err
	}
	data, err := conn.codec.EncodeMessage(&peer.Message{
		Header:  &peer.Header{SourceId: s.serverId, TargetId: conn.sourceId, Type: peer.MessageTypeICEAnswer, Sequence: conn.signalSend.Add(1)},
		Payload: &peer.Payload{ICEUsername: description.UsernameFragment, ICEPassword: description.Password, ICECandidates: append([]string(nil), description.Candidates...)},
	})
	if err != nil {
		return err
	}
	return s.api.SignalingServer.Publish(s.api.RoomId, signaling.MessageAnswer, data)
}
