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

func (s *Server) handleClientHello(data []byte) (*Connection, error) {
	header, err := s.defaultCodec.DecodeMessageHeader(data)
	if err != nil {
		return nil, err
	}

	if header.TargetId == "" && header.Type == peer.MessageTypeClientHello {
		existing, hasExisting := s.connections.Get(header.SourceId)
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
		handshakeCodec, err := peer.NewCodec(s.options.EncryptionType, key, s.options.EncryptionAAD, s.options.UseCompression)
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
		timestamp, err := time.Parse(time.RFC3339Nano, string(message.Payload.Timestamp))
		if err != nil {
			return nil, fmt.Errorf("invalid timestamp from %s: %w", header.SourceId, err)
		}
		if delta := s.options.GetTimeFunc().Sub(timestamp); delta > s.options.ClockSkew || delta < -s.options.ClockSkew {
			return nil, fmt.Errorf("timestamp from %s is outside allowed clock skew", header.SourceId)
		}
		if hasExisting {
			if existing.transportConnValue() == nil && existing.selectedTransport == "" &&
				bytes.Equal(existing.clientNonce, nonce) &&
				bytes.Equal(existing.clientEphemeralKey, message.Payload.ClientEphemeralKey) {
				// QoS transports may duplicate an authenticated hello. Re-send the
				// same pending session without accepting a different proof.
				return existing, nil
			}
			// A new hello has already been authenticated with the client's PSK.
			// Replace the previous session so reconnect does not depend on the
			// old transport or an asynchronous disconnect callback noticing EOF
			// before this hello arrives.
			_ = existing.Close()
		}
		if !s.isNonceAvailable(nonce) {
			return nil, fmt.Errorf("received replayed nonce from %s", header.SourceId)
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
		codec, err := peer.NewCodec(s.options.EncryptionType, sessionKey, s.options.EncryptionAAD, s.options.UseCompression)
		if err != nil {
			return nil, err
		}
		conn := &Connection{
			sourceId:           header.SourceId,
			codec:              codec,
			handshakeCodec:     handshakeCodec,
			sessionSalt:        sessionSalt,
			clientNonce:        append([]byte(nil), nonce...),
			serverNonce:        serverNonce,
			clientEphemeralKey: append([]byte(nil), message.Payload.ClientEphemeralKey...),
			serverEphemeralKey: serverPublicKey,
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

func (s *Server) sendServerHello(codec *peer.Codec, sourceId string, sessionSalt []byte) error {
	conn, ok := s.connections.Get(sourceId)
	if !ok {
		return errors.New("client session is not installed")
	}
	signature, err := peer.SignServerHello(
		s.options.IdentityKey,
		s.api.RoomId,
		sourceId,
		s.serverId,
		conn.clientNonce,
		conn.serverNonce,
		conn.clientEphemeralKey,
		conn.serverEphemeralKey,
		sessionSalt,
	)
	if err != nil {
		return err
	}
	dataBytes, err := codec.EncodeMessage(&peer.Message{
		Header: &peer.Header{
			Type:     peer.MessageTypeServerHello,
			SourceId: s.serverId,
			TargetId: sourceId,
		},
		Payload: &peer.Payload{
			SessionSalt:        sessionSalt,
			ClientNonce:        conn.clientNonce,
			ServerNonce:        conn.serverNonce,
			ClientEphemeralKey: conn.clientEphemeralKey,
			ServerEphemeralKey: conn.serverEphemeralKey,
			Signature:          signature,
			Transports:         transportNames(s.options.Transports),
		},
		Encrypt: true,
	})
	if err != nil {
		return err
	}

	if err = s.api.SignalingServer.Publish(s.api.RoomId, signaling.MessageHello, dataBytes); err != nil {
		return err
	}

	return nil
}
