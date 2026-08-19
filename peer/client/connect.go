package client

import (
	"context"
	"encoding/binary"
	"errors"
	"io"
	"net"
	"time"

	"github.com/anyshake/telekit/peer"
	"github.com/anyshake/telekit/relays"
	"github.com/anyshake/telekit/signaling"
	"github.com/anyshake/telekit/transports"
	"github.com/pion/ice/v4"
	"github.com/samber/lo"
)

func (c *Client) setIsConnected(v bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.connected = v
}

func (c *Client) isConnected() bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.connected
}

func (c *Client) Connect() error {
	ctx, cancel := context.WithTimeout(context.Background(), c.options.Timeout)
	defer cancel()
	return c.ConnectWithContext(ctx)
}

func (c *Client) ConnectWithContext(ctx context.Context) error {
	c.manualDisconnect.Store(false)
	return c.connectWithContext(ctx, false)
}

func (c *Client) connectWithContext(ctx context.Context, preserveReceiveBuffer bool) error {
	c.mu.Lock()
	if c.connected || c.connecting {
		c.mu.Unlock()
		return errors.New("connection already established")
	}
	c.connecting = true
	c.mu.Unlock()
	defer func() {
		c.mu.Lock()
		c.connecting = false
		c.mu.Unlock()
	}()

	c.stateMu.Lock()
	c.serverId = ""
	c.codec = nil
	c.localAddr = peer.Addr{}
	c.remoteAddr = peer.Addr{}
	c.stateMu.Unlock()
	c.signalSend.Store(0)
	c.signalRecv.Reset()
	succeeded := false
	defer func() {
		if !succeeded {
			_ = c.disconnect(false, !preserveReceiveBuffer)
		}
	}()
	buffer := c.recvBuf.Load()
	if buffer == nil || buffer.IsClosed() {
		c.recvBuf.Store(peer.NewRecvBufferWithLimit(c.options.ReceiveBufferSize, nil))
	} else {
		buffer.Reset()
	}

	attempt, err := c.buildClientHello()
	if err != nil {
		return err
	}
	handshakeCh := make(chan *serverHandshake, 1)
	sub, err := c.api.SignalingServer.Subscribe(c.api.RoomId, signaling.MessageHello, func(data []byte) {
		handshake, err := c.handleServerHello(data, attempt)
		if err != nil {
			return
		}
		select {
		case handshakeCh <- handshake:
		default:
		}
	})
	if err != nil {
		return err
	}
	defer sub.Unsubscribe()
	if err := c.api.SignalingServer.Publish(c.api.RoomId, signaling.MessageHello, attempt.message); err != nil {
		return err
	}
	if c.options.OnClientHello != nil {
		c.options.OnClientHello(c)
	}

	var handshake *serverHandshake
	helloRetry := time.NewTicker(time.Second)
	defer helloRetry.Stop()
	for handshake == nil {
		select {
		case handshake = <-handshakeCh:
			c.stateMu.Lock()
			c.codec = handshake.codec
			c.serverId = handshake.serverID
			c.stateMu.Unlock()
			if c.options.OnServerHello != nil {
				c.options.OnServerHello(c)
			}
		case <-helloRetry.C:
			// A network switch can leave the signaling transport half-open:
			// Publish may succeed locally while the server never receives the
			// packet. Re-send the same authenticated hello until ServerHello
			// arrives. The server treats the same nonce as an idempotent retry.
			_ = c.api.SignalingServer.Publish(c.api.RoomId, signaling.MessageHello, attempt.message)
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	if !lo.Contains(handshake.transports, c.options.Transport.Name()) {
		return errors.New("requested transport is not supported by server")
	}
	answers := make(chan transports.ICEDescription, 1)
	answerSub, err := c.api.SignalingServer.Subscribe(c.api.RoomId, signaling.MessageAnswer, func(data []byte) {
		msg, err := handshake.codec.DecodeMessage(data)
		if err != nil || msg.Header.SourceId != handshake.serverID || msg.Header.TargetId != c.clientId || !c.signalRecv.Accept(msg.Header.Sequence) {
			return
		}
		if msg.Header.Type != peer.MessageTypeICEAnswer || msg.Payload == nil || msg.Payload.ICEUsername == "" || msg.Payload.ICEPassword == "" {
			return
		}
		answer := transports.ICEDescription{UsernameFragment: msg.Payload.ICEUsername, Password: msg.Payload.ICEPassword, Candidates: msg.Payload.ICECandidates}
		if err := transports.ValidateICEDescriptionLimits(answer, c.options.MaxPendingICE, c.options.MaxPendingICEBytes); err != nil {
			return
		}
		answer.Candidates = append([]string(nil), answer.Candidates...)
		if c.options.OnICEAnswer != nil {
			c.options.OnICEAnswer(c, answer)
		}
		select {
		case answers <- answer:
		default:
		}
	})
	if err != nil {
		return err
	}
	defer answerSub.Unsubscribe()

	if err := c.publishTransportSelect(); err != nil {
		return err
	}
	relayProvider, err := c.api.WebSocketRelayProvider(c.clientId, c.serverId)
	if err != nil {
		return err
	}
	agentOptions := append([]ice.AgentOption(nil), c.options.ICEAgentOptions...)
	agent, err := transports.NewICEAgent(c.api.ICEURLs, agentOptions...)
	if err != nil {
		return err
	}
	if !c.installICEAgent(agent) {
		return net.ErrClosed
	}
	description, err := transports.GatherICEWithCallback(agent, func(candidate ice.Candidate) {
		if candidate == nil {
			if c.options.OnICECandidateGatheringComplete != nil {
				c.options.OnICECandidateGatheringComplete(c)
			}
			return
		}
		if c.options.OnICECandidate != nil {
			c.options.OnICECandidate(c, candidate)
		}
	}, func() error {
		if relayProvider == nil {
			return nil
		}
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
		return err
	}
	if err := transports.ValidateICEDescriptionLimits(description, c.options.MaxPendingICE, c.options.MaxPendingICEBytes); err != nil {
		return err
	}
	if c.options.OnICEOffer != nil {
		c.options.OnICEOffer(c, description)
	}
	if err := c.publishICEOffer(description); err != nil {
		return err
	}

	var remote transports.ICEDescription
	select {
	case remote = <-answers:
	case <-ctx.Done():
		return ctx.Err()
	}
	endpointLocal, endpointRemote := c.LocalAddr(), c.RemoteAddr()
	iceConn, err := transports.DialICE(ctx, agent, remote)
	if err != nil {
		return err
	}
	c.setPhysicalAddrs(iceConn.LocalAddr(), iceConn.RemoteAddr())
	selected := c.options.Transport
	transportKey := handshake.codec.SessionKey()
	endpoint := transports.ICEEndpoint(iceConn, endpointLocal, endpointRemote, transportKey)
	dataConn, err := selected.Dial(ctx, endpoint)
	if err != nil {
		_ = iceConn.Close()
		return err
	}
	if !c.setTransportConn(agent, dataConn, handshake.dataChannel) {
		return net.ErrClosed
	}
	c.lastTransportRead.Store(c.options.GetTimeFunc().UnixNano())
	recvBuf := c.recvBuf.Load()
	go c.readTransport(dataConn, handshake.dataChannel, recvBuf, transportPacketMode(selected))
	go c.monitorTransport(dataConn, handshake.dataChannel)
	c.setIsConnected(true)
	succeeded = true
	if c.options.OnConnected != nil {
		c.options.OnConnected(c)
	}
	return nil
}

func (c *Client) publishTransportSelect() error {
	c.stateMu.RLock()
	codec, serverID := c.codec, c.serverId
	c.stateMu.RUnlock()
	if codec == nil || serverID == "" {
		return errors.New("signaling session is not established")
	}
	data, err := codec.EncodeMessage(&peer.Message{
		Header:  &peer.Header{SourceId: c.clientId, TargetId: serverID, Type: peer.MessageTypeTransportSelect, Sequence: c.signalSend.Add(1)},
		Payload: &peer.Payload{Transport: c.options.Transport.Name()},
	})
	if err != nil {
		return err
	}
	return c.api.SignalingServer.Publish(c.api.RoomId, signaling.MessageOffer, data)
}

func (c *Client) publishICEOffer(description transports.ICEDescription) error {
	if err := transports.ValidateICEDescriptionLimits(description, c.options.MaxPendingICE, c.options.MaxPendingICEBytes); err != nil {
		return err
	}
	c.stateMu.RLock()
	codec, serverID := c.codec, c.serverId
	c.stateMu.RUnlock()
	if codec == nil || serverID == "" {
		return errors.New("signaling session is not established")
	}
	data, err := codec.EncodeMessage(&peer.Message{
		Header:  &peer.Header{SourceId: c.clientId, TargetId: serverID, Type: peer.MessageTypeICEOffer, Sequence: c.signalSend.Add(1)},
		Payload: &peer.Payload{ICEUsername: description.UsernameFragment, ICEPassword: description.Password, ICECandidates: append([]string(nil), description.Candidates...)},
	})
	if err != nil {
		return err
	}
	return c.api.SignalingServer.Publish(c.api.RoomId, signaling.MessageOffer, data)
}

func (c *Client) readTransport(conn net.Conn, dataChannel *peer.DataChannel, recvBuf *peer.RecvBuffer, packetMode bool) {
	if packetMode {
		c.readRawTransport(conn, dataChannel, recvBuf)
		return
	}

	var header [8]byte
	var sequenceBytes [peer.DataFrameSequenceSize]byte
	var frame []byte
	for {
		if _, err := io.ReadFull(conn, header[:]); err != nil {
			break
		}
		if !c.isCurrentTransport(conn, dataChannel) {
			break
		}
		length := binary.BigEndian.Uint64(header[:])
		if length == 0 {
			break
		}
		if length > uint64(c.options.MaxFrameSize) {
			break
		}
		if _, err := io.ReadFull(conn, sequenceBytes[:]); err != nil {
			break
		}
		if cap(frame) < int(length) {
			frame = make([]byte, length)
		} else {
			frame = frame[:length]
		}
		if _, err := io.ReadFull(conn, frame); err != nil {
			break
		}
		sequence := binary.BigEndian.Uint64(sequenceBytes[:])
		frameType, raw, err := dataChannel.OpenFrame(sequence, frame, c.options.MaxFrameSize)
		if err != nil || len(raw) > c.options.MaxFrameSize {
			break
		}
		if !c.isCurrentTransport(conn, dataChannel) {
			break
		}
		c.lastTransportRead.Store(c.options.GetTimeFunc().UnixNano())
		if frameType == peer.DataFrameHeartbeat {
			continue
		}
		if err := recvBuf.Write(raw); err != nil {
			break
		}
	}
	c.finishTransport(conn, dataChannel)
}

func (c *Client) readRawTransport(conn net.Conn, dataChannel *peer.DataChannel, recvBuf *peer.RecvBuffer) {
	packet := make([]byte, c.options.MaxFrameSize+8+peer.DataFrameSequenceSize)
	for {
		n, err := conn.Read(packet)
		if err != nil {
			break
		}
		if !c.isCurrentTransport(conn, dataChannel) {
			break
		}
		if n < 8 {
			break
		}
		length := binary.BigEndian.Uint64(packet[:8])
		if length == 0 {
			break
		}
		if n < 8+peer.DataFrameSequenceSize || length > uint64(c.options.MaxFrameSize) || int(length) != n-8-peer.DataFrameSequenceSize {
			break
		}
		sequenceOffset := 8 + peer.DataFrameSequenceSize
		sequence := binary.BigEndian.Uint64(packet[8:sequenceOffset])
		frameType, raw, err := dataChannel.OpenFrame(sequence, packet[sequenceOffset:n], c.options.MaxFrameSize)
		if errors.Is(err, peer.ErrDataFrameReplay) {
			continue
		}
		if err != nil || len(raw) > c.options.MaxFrameSize {
			break
		}
		if !c.isCurrentTransport(conn, dataChannel) {
			break
		}
		c.lastTransportRead.Store(c.options.GetTimeFunc().UnixNano())
		if frameType == peer.DataFrameHeartbeat {
			continue
		}
		if err := recvBuf.Write(raw); err != nil {
			break
		}
	}
	c.finishTransport(conn, dataChannel)
}

func (c *Client) monitorTransport(conn net.Conn, dataChannel *peer.DataChannel) {
	ticker := time.NewTicker(c.options.HeartbeatInterval)
	defer ticker.Stop()
	for range ticker.C {
		if !c.isCurrentTransport(conn, dataChannel) {
			return
		}
		lastRead := time.Unix(0, c.lastTransportRead.Load())
		if c.options.GetTimeFunc().Sub(lastRead) >= c.options.HeartbeatTimeout {
			_ = c.finishTransport(conn, dataChannel)
			return
		}
		if err := c.sendHeartbeat(conn, dataChannel); err != nil {
			_ = c.finishTransport(conn, dataChannel)
			return
		}
	}
}

func transportPacketMode(transport transports.ITransport) bool {
	behavior, ok := transport.(transports.PacketModeTransport)
	return ok && behavior.PacketMode()
}

func transportMaxFrameSize(transport transports.ITransport) int {
	behavior, ok := transport.(transports.MaxFrameSizeTransport)
	if !ok {
		return 0
	}
	return behavior.MaxFrameSize()
}
