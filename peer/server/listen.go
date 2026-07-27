package server

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/anyshake/telekit/peer"
	"github.com/anyshake/telekit/signaling"
	transportcore "github.com/anyshake/telekit/transports"
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
		if !s.helloLimiter.Allow() {
			return
		}
		conn, err := s.handleClientHello(data)
		if conn == nil || err != nil {
			return
		}
		s.connections.Set(conn.sourceId, conn)
		conn.startHandshakeTimer(s.options.HandshakeTimeout)
		if err := s.sendServerHello(conn.handshakeCodec, conn.sourceId, conn.sessionSalt); err != nil {
			s.connections.Del(conn.sourceId)
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
			if conn.selectedTransport == "" {
				conn.selectedTransport = msg.Payload.Transport
			}
			if conn.pendingICEOffer != nil {
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
				Candidates:       append([]string(nil), msg.Payload.ICECandidates...),
			}
			conn.stateMu.Lock()
			selected := conn.selectedTransport != ""
			if !selected {
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
	conn.stateMu.RLock()
	selectedName := conn.selectedTransport
	conn.stateMu.RUnlock()
	if selectedName == "" {
		return
	}
	agent, err := transportcore.NewICEAgent(s.api.ICEURLs)
	if err != nil {
		_ = conn.Close()
		return
	}
	description, err := transportcore.GatherICE(agent)
	if err != nil {
		_ = agent.Close()
		_ = conn.Close()
		return
	}
	conn.stateMu.Lock()
	conn.iceAgent = agent
	conn.stateMu.Unlock()
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
	selected := findTransport(s.options.Transports, selectedName)
	if selected == nil {
		_ = iceConn.Close()
		_ = conn.Close()
		return
	}
	endpointLocal := peer.Addr{RoomID: conn.roomId, PeerID: conn.serverId}
	endpointRemote := peer.Addr{RoomID: conn.roomId, PeerID: conn.sourceId}
	conn.setPhysicalAddrs(iceConn.LocalAddr(), iceConn.RemoteAddr())
	dataConn, err := selected.Accept(ctx, transportcore.ICEEndpoint(iceConn, endpointLocal, endpointRemote))
	if err != nil {
		_ = iceConn.Close()
		_ = conn.Close()
		return
	}
	conn.lastTransportRead.Store(time.Now().UnixNano())
	conn.setTransportConn(dataConn)
	go conn.readTransport(dataConn, selected.Name() == "raw_udp")
	conn.markEstablished()
	go monitorTransport(conn)
	select {
	case s.acceptCh <- conn:
	case <-s.closeCh:
		_ = conn.Close()
	default:
		_ = conn.Close()
	}
}

func monitorTransport(conn *Connection) {
	ticker := time.NewTicker(transportKeepaliveInterval)
	defer ticker.Stop()
	for range ticker.C {
		if conn.transportConnValue() == nil {
			return
		}
		lastRead := time.Unix(0, conn.lastTransportRead.Load())
		if time.Since(lastRead) >= transportKeepaliveTimeout {
			_ = conn.Close()
			return
		}
		if err := conn.sendHeartbeat(); err != nil {
			_ = conn.Close()
			return
		}
	}
}

func (s *Server) sendICEAnswer(conn *Connection, description transportcore.ICEDescription) error {
	data, err := conn.codec.EncodeMessage(&peer.Message{
		Header:  &peer.Header{SourceId: s.serverId, TargetId: conn.sourceId, Type: peer.MessageTypeICEAnswer, Sequence: conn.signalSend.Add(1)},
		Payload: &peer.Payload{ICEUsername: description.UsernameFragment, ICEPassword: description.Password, ICECandidates: description.Candidates},
	})
	if err != nil {
		return err
	}
	return s.api.SignalingServer.Publish(s.api.RoomId, signaling.MessageAnswer, data)
}
