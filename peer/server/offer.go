package server

import (
	"bytes"
	"encoding/binary"
	"errors"

	"github.com/anyshake/telekit/peer"
	"github.com/pion/webrtc/v4"
)

func (s *Server) handleOffer(conn *Connection, msg *peer.Message) error {
	if msg == nil || msg.Payload == nil || msg.Payload.SDP == nil || msg.Payload.SDP.Type != webrtc.SDPTypeOffer {
		return errors.New("offer SDP is missing or has the wrong type")
	}
	conn.signalMu.Lock()
	defer conn.signalMu.Unlock()
	conn.handshakeCodec = nil

	pc := conn.peerConnection()
	if pc == nil {
		return errors.New("peer connection is closed")
	}
	pc.OnICECandidate(func(c *webrtc.ICECandidate) {
		if c != nil {
			s.sendICECandidate(conn, c.ToJSON())
		}
	})
	pc.OnConnectionStateChange(func(state webrtc.PeerConnectionState) {
		switch state {
		case webrtc.PeerConnectionStateClosed, webrtc.PeerConnectionStateFailed:
			s.connections.Del(conn.sourceId)
			conn.setDataChannel(nil)
			conn.recvBuf.Close()
			conn.releaseLease()

			if s.options.OnDisconnected != nil {
				s.options.OnDisconnected(conn)
			}

			if state == webrtc.PeerConnectionStateFailed && s.options.OnConnectionFailed != nil {
				s.options.OnConnectionFailed(conn, errors.New("peer connection failed"))
			}
		}
	})
	pc.OnDataChannel(func(dc *webrtc.DataChannel) {
		if dc.Label() != s.api.DataChannel {
			return
		}
		conn.setDataChannel(dc)

		dc.OnOpen(func() {
			conn.markEstablished()
			if !s.options.ReceiveEventsOnly {
				select {
				case s.acceptCh <- conn:
				case <-s.closeCh:
				default:
					go conn.Close()
					return
				}
			}
			if s.options.OnDataChannelOpen != nil {
				s.options.OnDataChannelOpen(conn)
			}
		})
		dc.OnMessage(func(m webrtc.DataChannelMessage) {
			if len(m.Data) == 0 {
				return
			}

			conn.dataChunkBuf.mu.Lock()
			defer conn.dataChunkBuf.mu.Unlock()

			buf := m.Data

			if conn.dataChunkBuf.expectedLen == 0 {
				if len(buf) < 8 {
					return
				}
				conn.dataChunkBuf.expectedLen = binary.BigEndian.Uint64(buf[:8])
				if conn.dataChunkBuf.expectedLen == 0 || conn.dataChunkBuf.expectedLen > uint64(s.options.MaxFrameSize) {
					conn.resetDataChunkLocked()
					go conn.Close()
					return
				}
				if !s.bufferBudget.Reserve(int(conn.dataChunkBuf.expectedLen)) {
					conn.resetDataChunkLocked()
					go conn.Close()
					return
				}
				conn.dataChunkBuf.reserved = int(conn.dataChunkBuf.expectedLen)
				buf = buf[8:]
			}

			if uint64(len(buf)) > conn.dataChunkBuf.expectedLen-uint64(conn.dataChunkBuf.recvBuffer.Len()) {
				conn.resetDataChunkLocked()
				go conn.Close()
				return
			}
			conn.dataChunkBuf.recvBuffer.Write(buf)

			if uint64(conn.dataChunkBuf.recvBuffer.Len()) >= conn.dataChunkBuf.expectedLen {
				data := conn.dataChunkBuf.recvBuffer.Bytes()[:conn.dataChunkBuf.expectedLen]
				conn.dataChunkBuf.recvBuffer = bytes.Buffer{}
				conn.dataChunkBuf.expectedLen = 0

				raw, err := conn.codec.DecodeWithDecryptionLimit(data, s.options.MaxFrameSize)
				s.bufferBudget.Release(conn.dataChunkBuf.reserved)
				conn.dataChunkBuf.reserved = 0
				if err != nil {
					return
				}
				if len(raw) > s.options.MaxFrameSize {
					go conn.Close()
					return
				}
				if !s.options.ReceiveEventsOnly {
					if err := conn.recvBuf.Write(raw); err != nil {
						go conn.Close()
						return
					}
				}
				if s.options.OnDataChannelMessage != nil {
					if !s.submitDataCallback(conn, raw) {
						go conn.Close()
					}
				}
			}
		})
		dc.OnClose(func() {
			conn.setDataChannel(nil)
			go conn.Close()
			if s.options.OnDataChannelClose != nil {
				s.options.OnDataChannelClose(conn)
			}
		})
		dc.OnError(func(err error) {
			conn.setDataChannel(nil)
			go conn.Close()
			if s.options.OnDataChannelError != nil {
				s.options.OnDataChannelError(conn, err)
			}
		})
	})

	if err := pc.SetRemoteDescription(*msg.Payload.SDP); err != nil {
		return err
	}
	conn.remoteSet = true

	for _, ice := range conn.pendingICE {
		_ = pc.AddICECandidate(ice)
	}
	conn.pendingICE = nil
	s.bufferBudget.Release(conn.pendingICEBytes)
	conn.pendingICEBytes = 0

	if err := s.sendAnswer(conn, msg); err != nil {
		return err
	}

	return nil
}
