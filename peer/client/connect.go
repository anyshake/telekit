package client

import (
	"context"
	"encoding/binary"
	"errors"
	"io"
	"net"
	"time"

	"github.com/anyshake/telekit/peer"
	"github.com/anyshake/telekit/signaling"
	"github.com/anyshake/telekit/transports"
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
			_ = c.Disconnect()
		}
	}()
	c.recvBuf.Store(peer.NewRecvBufferWithLimit(c.options.ReceiveBufferSize, nil))

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
	select {
	case handshake = <-handshakeCh:
		c.codec = handshake.codec
		c.serverId = handshake.serverID
		if c.options.OnServerHello != nil {
			c.options.OnServerHello(c)
		}
	case <-ctx.Done():
		return ctx.Err()
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
		select {
		case answers <- transports.ICEDescription{UsernameFragment: msg.Payload.ICEUsername, Password: msg.Payload.ICEPassword, Candidates: append([]string(nil), msg.Payload.ICECandidates...)}:
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
	agent, err := transports.NewICEAgent(c.api.ICEURLs)
	if err != nil {
		return err
	}
	c.stateMu.Lock()
	c.iceAgent = agent
	c.stateMu.Unlock()
	description, err := transports.GatherICE(agent)
	if err != nil {
		return err
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
	dataConn, err := selected.Dial(ctx, transports.ICEEndpoint(iceConn, endpointLocal, endpointRemote))
	if err != nil {
		_ = iceConn.Close()
		return err
	}
	c.setTransportConn(dataConn)
	c.lastTransportRead.Store(time.Now().UnixNano())
	go c.readTransport(dataConn, selected.Name() == "raw_udp")
	go c.monitorTransport(dataConn)
	c.setIsConnected(true)
	succeeded = true
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
	for {
		if _, err := io.ReadFull(conn, header[:]); err != nil {
			break
		}
		c.lastTransportRead.Store(time.Now().UnixNano())
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
		frame := make([]byte, length)
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
		c.lastTransportRead.Store(time.Now().UnixNano())
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
		if time.Since(lastRead) >= transportKeepaliveTimeout {
			_ = c.finishTransport(conn)
			return
		}
		if err := c.sendHeartbeat(); err != nil {
			_ = c.finishTransport(conn)
			return
		}
	}
}
