package server

import (
	"bytes"
	"crypto/ecdh"
	"crypto/rand"
	"errors"
	"fmt"
	"time"

	"github.com/anyshake/telekit/peer"
	"github.com/anyshake/telekit/signaling"
)

var errReplayedClientHello = errors.New("client hello nonce was replayed")

// prepareClientHello validates a new hello before replacing an existing
// session. A replay must not be allowed to close the currently active
// connection.
func (s *Server) prepareClientHello(existing *Connection, nonce, clientEphemeralKey []byte) (bool, error) {
	pending := false
	if existing != nil {
		existing.stateMu.RLock()
		pending = !existing.closed.Load() && existing.transportConn == nil && existing.selectedTransport == ""
		existing.stateMu.RUnlock()
	}
	if pending &&
		bytes.Equal(existing.clientNonce, nonce) && bytes.Equal(existing.clientEphemeralKey, clientEphemeralKey) {
		// QoS transports may duplicate an authenticated hello. Re-send the
		// same pending session without accepting a different proof.
		return true, nil
	}
	if !s.isNonceAvailable(nonce) {
		return false, errReplayedClientHello
	}
	if existing != nil {
		// The nonce is fresh, so this is a legitimate new session. Replace the
		// previous session without depending on EOF propagation from its old
		// transport.
		_ = existing.Close()
	}
	return false, nil
}

func (s *Server) handleClientHello(data []byte) (*Connection, error) {
	header, err := s.defaultCodec.DecodeMessageHeader(data)
	if err != nil {
		return nil, err
	}

	if header.TargetId == "" && header.Type == peer.MessageTypeClientHello {
		if len(header.HandshakeNonce) != peer.HandshakeNonceSize {
			return nil, errors.New("invalid client hello nonce")
		}
		existing, _ := s.connections.Get(header.SourceId)
		if !s.isClientIdValid(header.SourceId) {
			err := fmt.Errorf("rejected connection from %s due to server restrictions", header.SourceId)
			if s.options.OnNewClientReject != nil {
				s.options.OnNewClientReject(header.SourceId, err)
			}
			return nil, err
		}
		key, err := s.options.KeyProvider.Key(header.SourceId)
		if err != nil {
			if s.options.OnNewClientReject != nil {
				s.options.OnNewClientReject(header.SourceId, err)
			}
			return nil, err
		}
		if err := peer.ValidateClientCredentials(header.SourceId, key); err != nil {
			return nil, err
		}
		handshakeKey, err := peer.DeriveClientHelloKey(key, s.api.RoomId, header.SourceId, s.serverId, header.HandshakeNonce)
		if err != nil {
			return nil, err
		}
		handshakeCodec, err := peer.NewCodec(s.options.EncryptionType, handshakeKey, s.options.EncryptionAAD)
		if err != nil {
			return nil, err
		}

		message, err := handshakeCodec.DecodeMessage(data)
		if err != nil {
			return nil, fmt.Errorf("invalid client proof from %s: %w", header.SourceId, err)
		}
		if message.Payload.HandshakeRoomID != s.api.RoomId || message.Payload.HandshakeClientID != header.SourceId {
			return nil, fmt.Errorf("client proof is not bound to room and client identity")
		}
		clientPublicKey, err := ecdh.X25519().NewPublicKey(message.Payload.ClientEphemeralKey)
		if err != nil {
			return nil, fmt.Errorf("invalid client X25519 public key")
		}
		nonce := message.Payload.ClientNonce
		if len(nonce) != peer.HandshakeNonceSize {
			err = fmt.Errorf("received invalid nonce from %s", header.SourceId)
			if s.options.OnNewClientReject != nil {
				s.options.OnNewClientReject(header.SourceId, err)
			}
			return nil, err
		}
		if !bytes.Equal(header.HandshakeNonce, nonce) {
			return nil, errors.New("client hello nonce does not match authenticated payload")
		}
		timestamp, err := time.Parse(time.RFC3339Nano, string(message.Payload.Timestamp))
		if err != nil {
			return nil, fmt.Errorf("invalid timestamp from %s: %w", header.SourceId, err)
		}
		if delta := s.options.GetTimeFunc().Sub(timestamp); delta > s.options.ClockSkew || delta < -s.options.ClockSkew {
			return nil, fmt.Errorf("timestamp from %s is outside allowed clock skew", header.SourceId)
		}
		reuse, err := s.prepareClientHello(existing, nonce, message.Payload.ClientEphemeralKey)
		if err != nil {
			return nil, fmt.Errorf("received replayed nonce from %s", header.SourceId)
		}
		if reuse {
			return existing, nil
		}
		if s.options.OnNewClientJoin != nil {
			if accept := s.options.OnNewClientJoin(s.connections, header.SourceId); !accept {
				err = fmt.Errorf("rejected connection from %s due to server policy", header.SourceId)
				if s.options.OnNewClientReject != nil {
					s.options.OnNewClientReject(header.SourceId, err)
				}
				return nil, err
			}
		}
		if !s.reserveConnection() {
			err = errors.New("server connection or pending-handshake limit reached")
			if s.options.OnNewClientReject != nil {
				s.options.OnNewClientReject(header.SourceId, err)
			}
			return nil, err
		}
		leaseTransferred := false
		defer func() {
			if !leaseTransferred {
				s.pendingHandshakes.Add(-1)
				s.totalConnections.Add(-1)
			}
		}()
		sessionSalt, err := createRandomHandshakeValue()
		if err != nil {
			return nil, err
		}
		serverNonce, err := createRandomHandshakeValue()
		if err != nil {
			return nil, err
		}
		serverHelloKey, err := peer.DeriveServerHelloKey(key, s.api.RoomId, header.SourceId, s.serverId, nonce, serverNonce)
		if err != nil {
			return nil, err
		}
		serverHelloCodec, err := peer.NewCodec(s.options.EncryptionType, serverHelloKey, s.options.EncryptionAAD)
		if err != nil {
			return nil, err
		}
		serverPrivateKey, err := ecdh.X25519().GenerateKey(rand.Reader)
		if err != nil {
			return nil, err
		}
		serverPublicKey := serverPrivateKey.PublicKey().Bytes()
		sharedSecret, err := serverPrivateKey.ECDH(clientPublicKey)
		if err != nil {
			return nil, fmt.Errorf("invalid client X25519 public key")
		}
		sessionKey, err := peer.DeriveSessionKey(
			key,
			sharedSecret,
			sessionSalt,
			s.api.RoomId,
			header.SourceId,
			s.serverId,
			nonce,
			serverNonce,
			message.Payload.ClientEphemeralKey,
			serverPublicKey,
		)
		if err != nil {
			return nil, err
		}
		codec, err := peer.NewSignalingChannel(s.options.EncryptionType, sessionKey, s.options.EncryptionAAD, peer.DataRoleServer)
		if err != nil {
			return nil, err
		}
		dataChannel, err := peer.NewDataChannel(
			s.options.EncryptionType,
			sessionKey,
			s.options.EncryptionAAD,
			peer.DataRoleServer,
		)
		if err != nil {
			return nil, err
		}
		conn := &Connection{
			sourceId:           header.SourceId,
			codec:              codec,
			handshakeCodec:     serverHelloCodec,
			sessionSalt:        sessionSalt,
			clientNonce:        append([]byte(nil), nonce...),
			serverNonce:        serverNonce,
			clientEphemeralKey: append([]byte(nil), message.Payload.ClientEphemeralKey...),
			serverEphemeralKey: serverPublicKey,
			dataChannel:        dataChannel,
			recvBuf:            peer.NewRecvBufferWithLimit(s.options.ReceiveBufferSize, s.bufferBudget),
			serverId:           s.serverId,
			roomId:             s.api.RoomId,
			owner:              s,
		}
		conn.pendingLease.Store(true)
		conn.totalLease.Store(true)
		leaseTransferred = true
		return conn, nil
	}

	return nil, nil
}

func (s *Server) sendServerHello(conn *Connection) error {
	if conn == nil {
		return errors.New("client session is nil")
	}
	signature, err := peer.SignServerHello(
		s.options.IdentityKey,
		s.api.RoomId,
		conn.sourceId,
		s.serverId,
		conn.clientNonce,
		conn.serverNonce,
		conn.clientEphemeralKey,
		conn.serverEphemeralKey,
		conn.sessionSalt,
	)
	if err != nil {
		return err
	}
	dataBytes, err := conn.handshakeCodec.EncodeMessage(&peer.Message{
		Header: &peer.Header{
			Type:           peer.MessageTypeServerHello,
			SourceId:       s.serverId,
			TargetId:       conn.sourceId,
			HandshakeNonce: conn.serverNonce,
		},
		Payload: &peer.Payload{
			SessionSalt:        conn.sessionSalt,
			ClientNonce:        conn.clientNonce,
			ServerNonce:        conn.serverNonce,
			ClientEphemeralKey: conn.clientEphemeralKey,
			ServerEphemeralKey: conn.serverEphemeralKey,
			Signature:          signature,
			Transports:         transportNames(s.options.Transports),
		},
	})
	if err != nil {
		return err
	}

	if err = s.api.SignalingServer.Publish(s.api.RoomId, signaling.MessageHello, dataBytes); err != nil {
		return err
	}

	return nil
}
