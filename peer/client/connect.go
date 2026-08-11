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

	codec, err := peer.NewCodec(c.options.EncryptionType, c.psk.Key, c.options.EncryptionAAD, c.options.UseCompression)
	if err != nil {
		return err
	}
	c.serverId = ""
	c.clearPhysicalAddrs()
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

	attempt, err := c.buildClientHello(codec)
	if err != nil {
		return err
	}
	handshakeCh := make(chan *serverHandshake, 1)
	sub, err := c.api.SignalingServer.Subscribe(c.api.RoomId, signaling.MessageHello, func(data []byte) {
		handshake, err := c.handleServerHello(codec, data, attempt)
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
			c.codec = handshake.codec
			c.serverId = handshake.serverID
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
		msg, err := c.codec.DecodeMessage(data)
		if err != nil || msg.Header.SourceId != c.serverId || msg.Header.TargetId != c.clientId || !c.signalRecv.Accept(msg.Header.Sequence) {
			return
		}
		if msg.Header.Type != peer.MessageTypeICEAnswer || msg.Payload == nil || msg.Payload.ICEUsername == "" || msg.Payload.ICEPassword == "" {
			return
		}
		answer := transports.ICEDescription{UsernameFragment: msg.Payload.ICEUsername, Password: msg.Payload.ICEPassword, Candidates: append([]string(nil), msg.Payload.ICECandidates...)}
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
	c.stateMu.Lock()
	c.iceAgent = agent
	c.stateMu.Unlock()
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
	_, transportKey := c.codec.GetSecret()
	endpoint := transports.ICEEndpoint(iceConn, endpointLocal, endpointRemote, transportKey)
	dataConn, err := selected.Dial(ctx, endpoint)
	if err != nil {
		_ = iceConn.Close()
		return err
	}
	c.setTransportConn(dataConn)
	c.lastTransportRead.Store(c.options.GetTimeFunc().UnixNano())
	go c.readTransport(dataConn, transportPacketMode(selected))
	go c.monitorTransport(dataConn)
	c.setIsConnected(true)
	succeeded = true
	if c.options.OnConnected != nil {
		c.options.OnConnected(c)
	}
	return nil
}

func (c *Client) publishTransportSelect() error {
	data, err := c.codec.EncodeMessage(&peer.Message{
		Header:  &peer.Header{SourceId: c.clientId, TargetId: c.serverId, Type: peer.MessageTypeTransportSelect, Sequence: c.signalSend.Add(1)},
		Payload: &peer.Payload{Transport: c.options.Transport.Name()},
	})
	if err != nil {
		return err
	}
	return c.api.SignalingServer.Publish(c.api.RoomId, signaling.MessageOffer, data)
}

func (c *Client) publishICEOffer(description transports.ICEDescription) error {
	data, err := c.codec.EncodeMessage(&peer.Message{
		Header:  &peer.Header{SourceId: c.clientId, TargetId: c.serverId, Type: peer.MessageTypeICEOffer, Sequence: c.signalSend.Add(1)},
		Payload: &peer.Payload{ICEUsername: description.UsernameFragment, ICEPassword: description.Password, ICECandidates: description.Candidates},
	})
	if err != nil {
		return err
	}
	return c.api.SignalingServer.Publish(c.api.RoomId, signaling.MessageOffer, data)
}

func (c *Client) readTransport(conn net.Conn, packetMode bool) {
	if packetMode {
		c.readRawTransport(conn)
		return
	}

	var header [8]byte
	var frame []byte
	for {
		if _, err := io.ReadFull(conn, header[:]); err != nil {
			break
		}
		c.lastTransportRead.Store(c.options.GetTimeFunc().UnixNano())
		length := binary.BigEndian.Uint64(header[:])
		if length == 0 {
			if err := c.sendHeartbeat(); err != nil {
				break
			}
			continue
		}
		if length > uint64(c.options.MaxFrameSize) {
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
		raw, err := c.codec.DecodeWithDecryptionLimit(frame, c.options.MaxFrameSize)
		if err != nil || len(raw) > c.options.MaxFrameSize {
			break
		}
		if err := c.recvBuf.Load().Write(raw); err != nil {
			break
		}
	}
	c.finishTransport(conn)
}

func (c *Client) readRawTransport(conn net.Conn) {
	packet := make([]byte, c.options.MaxFrameSize+8)
	for {
		n, err := conn.Read(packet)
		if err != nil {
			break
		}
		c.lastTransportRead.Store(c.options.GetTimeFunc().UnixNano())
		if n < 8 {
			break
		}
		length := binary.BigEndian.Uint64(packet[:8])
		if length == 0 {
			if n != 8 {
				break
			}
			if err := c.sendHeartbeat(); err != nil {
				break
			}
			continue
		}
		if length > uint64(c.options.MaxFrameSize) || int(length) != n-8 {
			break
		}
		raw, err := c.codec.DecodeWithDecryptionLimit(packet[8:n], c.options.MaxFrameSize)
		if err != nil || len(raw) > c.options.MaxFrameSize {
			break
		}
		if err := c.recvBuf.Load().Write(raw); err != nil {
			break
		}
	}
	c.finishTransport(conn)
}

func (c *Client) monitorTransport(conn net.Conn) {
	ticker := time.NewTicker(transportKeepaliveInterval)
	defer ticker.Stop()
	for range ticker.C {
		if c.transportConnValue() != conn {
			return
		}
		lastRead := time.Unix(0, c.lastTransportRead.Load())
		if c.options.GetTimeFunc().Sub(lastRead) >= transportKeepaliveTimeout {
			_ = c.finishTransport(conn)
			return
		}
		if err := c.sendHeartbeat(); err != nil {
			_ = c.finishTransport(conn)
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
