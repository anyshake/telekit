package client

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"

	"github.com/anyshake/telekit/peer"
	"github.com/anyshake/telekit/signaling"
	"github.com/pion/webrtc/v4"
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
	c.signalSend.Store(0)
	c.signalRecv.Reset()
	succeeded := false
	defer func() {
		if !succeeded {
			_ = c.Disconnect()
		}
	}()

	// Replace the receive buffer so Read() on the new connection
	// returns fresh data; any pending Read on the old buffer will
	// have already returned io.EOF via Disconnect().
	c.recvBuf.Store(peer.NewRecvBufferWithLimit(c.options.ReceiveBufferSize, nil))
	c.startCallbackPool()

	attempt, err := c.buildClientHello(codec)
	if err != nil {
		return err
	}
	handshakeCh := make(chan *serverHandshake, 1)
	sub, err := c.api.SignalingServer.Subscribe(c.api.RoomId, signaling.MessageHello, func(m []byte) {
		handshake, err := c.handleServerHello(codec, m, attempt)
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
	select {
	case handshake := <-handshakeCh:
		c.codec = handshake.codec
		c.serverId = handshake.serverID
		if c.options.OnServerHello != nil {
			c.options.OnServerHello(c)
		}
		c.signalMu.Lock()
		c.pendingICE = nil
		c.pendingICEBytes = 0
		c.remoteSet = false
		c.signalMu.Unlock()
		pc, err := c.api.WebRTCAPI.NewPeerConnection(c.api.WebRTCConfig)
		if err != nil {
			return err
		}
		dc, err := pc.CreateDataChannel(c.api.DataChannel, nil)
		if err != nil {
			_ = pc.Close()
			return err
		}
		c.setPeerConnection(pc, dc)
		break
	case <-ctx.Done():
		return ctx.Err()
	}

	pc := c.peerConnection()
	dc := c.dataChannel()
	pc.OnICECandidate(func(cand *webrtc.ICECandidate) {
		if cand != nil {
			_ = c.sendICECandidate(cand.ToJSON())
		}
	})
	pc.OnICEConnectionStateChange(func(state webrtc.ICEConnectionState) {
		switch state {
		case webrtc.ICEConnectionStateDisconnected, webrtc.ICEConnectionStateClosed:
			c.setIsConnected(false)
			c.recvBuf.Load().Close()
			if c.options.OnDisconnected != nil {
				c.options.OnDisconnected(c)
			}
		case webrtc.ICEConnectionStateFailed:
			c.setIsConnected(false)
			c.recvBuf.Load().Close()
			if c.options.OnConnectionFailed != nil {
				c.options.OnConnectionFailed(c, errors.New("ICE connection failed"))
			}
		}
	})
	pc.OnConnectionStateChange(func(state webrtc.PeerConnectionState) {
		switch state {
		case webrtc.PeerConnectionStateFailed, webrtc.PeerConnectionStateClosed:
			c.setIsConnected(false)
			c.recvBuf.Load().Close()
			if c.options.OnConnectionFailed != nil {
				c.options.OnConnectionFailed(c, errors.New("peerconnection failed"))
			}
			if c.options.OnDisconnected != nil {
				c.options.OnDisconnected(c)
			}
		}
	})

	openCh := make(chan struct{}, 1)
	dc.OnOpen(func() {
		c.setIsConnected(true)
		select {
		case openCh <- struct{}{}:
		default:
		}
		if c.options.OnDataChannelOpen != nil {
			c.options.OnDataChannelOpen(c)
		}
	})
	dc.OnClose(func() {
		c.setIsConnected(false)
		c.recvBuf.Load().Close()
		if c.options.OnDataChannelClose != nil {
			c.options.OnDataChannelClose(c)
		}
	})
	dc.OnError(func(err error) {
		c.setIsConnected(false)
		c.recvBuf.Load().Close()
		if c.options.OnDataChannelError != nil {
			c.options.OnDataChannelError(c, err)
		}
	})
	dc.OnMessage(func(m webrtc.DataChannelMessage) {
		if len(m.Data) == 0 {
			return
		}

		c.dataChunkBuf.mu.Lock()
		defer c.dataChunkBuf.mu.Unlock()

		buf := m.Data

		if c.dataChunkBuf.expectedLen == 0 {
			if len(buf) < 8 {
				return
			}
			c.dataChunkBuf.expectedLen = binary.BigEndian.Uint64(buf[:8])
			if c.dataChunkBuf.expectedLen == 0 || c.dataChunkBuf.expectedLen > uint64(c.options.MaxFrameSize) {
				c.dataChunkBuf.expectedLen = 0
				c.dataChunkBuf.recvBuffer = bytes.Buffer{}
				go c.Disconnect()
				return
			}
			buf = buf[8:]
		}

		if uint64(len(buf)) > c.dataChunkBuf.expectedLen-uint64(c.dataChunkBuf.recvBuffer.Len()) {
			c.dataChunkBuf.expectedLen = 0
			c.dataChunkBuf.recvBuffer = bytes.Buffer{}
			go c.Disconnect()
			return
		}
		c.dataChunkBuf.recvBuffer.Write(buf)

		if uint64(c.dataChunkBuf.recvBuffer.Len()) >= c.dataChunkBuf.expectedLen {
			data := c.dataChunkBuf.recvBuffer.Bytes()[:c.dataChunkBuf.expectedLen]
			c.dataChunkBuf.recvBuffer = bytes.Buffer{}
			c.dataChunkBuf.expectedLen = 0

			raw, err := c.codec.DecodeWithDecryptionLimit(data, c.options.MaxFrameSize)
			if err != nil {
				return
			}
			if len(raw) > c.options.MaxFrameSize {
				go c.Disconnect()
				return
			}
			if !c.options.ReceiveEventsOnly {
				if err := c.recvBuf.Load().Write(raw); err != nil {
					go c.Disconnect()
					return
				}
			}
			if c.options.OnDataChannelMessage != nil {
				if !c.submitDataCallback(raw) {
					go c.Disconnect()
				}
			}
		}
	})

	sub, err = c.api.SignalingServer.Subscribe(c.api.RoomId, signaling.MessageAnswer, func(b []byte) {
		msg, err := c.codec.DecodeMessage(b)
		if err != nil {
			return
		}
		if msg.Header.SourceId != c.serverId || msg.Header.TargetId != c.clientId {
			return
		}
		if !c.signalRecv.Accept(msg.Header.Sequence) {
			return
		}

		switch msg.Header.Type {
		case peer.MessageTypeAnswer:
			if msg.Payload.SDP == nil || msg.Payload.SDP.Type != webrtc.SDPTypeAnswer {
				return
			}
			if c.handleAnswer(msg) == nil && c.options.OnServerAnswer != nil {
				c.options.OnServerAnswer(c, msg.Payload.SDP)
			}
		case peer.MessageTypeICE:
			if msg.Payload.ICE == nil {
				return
			}
			_ = c.handleICECandidate(msg)
		}
	})
	if err != nil {
		return err
	}
	defer sub.Unsubscribe()

	if err := c.sendOffer(); err != nil {
		return err
	}
	select {
	case <-openCh:
		succeeded = true
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
