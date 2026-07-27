package main

import (
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/hex"
	"fmt"

	"github.com/anyshake/telekit/example/p2proxy/common"
	"github.com/anyshake/telekit/example/p2proxy/protocol"
	"github.com/anyshake/telekit/peer"
	peerapi "github.com/anyshake/telekit/peer/api"
	peerclient "github.com/anyshake/telekit/peer/client"
	"github.com/anyshake/telekit/signaling"
	"github.com/anyshake/telekit/utils/encryption"
)

func connectTelekit(args *arguments) (signaling.Adapter, *protocol.Pool, error) {
	if args.poolSize < 1 {
		return nil, nil, fmt.Errorf("pool-size must be positive")
	}
	transport, err := common.NewTransport(args.transportName)
	if err != nil {
		return nil, nil, err
	}
	serverPublicKey, err := decodePublicKey(args.serverPublicKey)
	if err != nil {
		return nil, nil, err
	}
	adapter, err := common.ConnectMQTT(args.mqttBroker, args.baseTopic, "client", args.queueMessages, args.queueBytes)
	if err != nil {
		return nil, nil, err
	}
	api, err := peerapi.NewAPI(args.room, adapter, common.APIOptions()...)
	if err != nil {
		adapter.Disconnect()
		return nil, nil, err
	}
	key := sha256.Sum256([]byte(args.secret))
	sessions := make([]*protocol.Session, 0, args.poolSize)
	for index := 0; index < args.poolSize; index++ {
		clientID := args.clientID
		if args.poolSize > 1 {
			clientID = fmt.Sprintf("%s-%d", args.clientID, index)
		}
		conn, err := peerclient.NewClient(peer.PreSharedKey{
			ClientID:        clientID,
			Key:             key[:],
			ServerPublicKey: serverPublicKey,
		}, api, &peerclient.Options{
			Timeout:        args.timeout,
			Transport:      transport,
			EncryptionType: encryption.AES_256_GCM,
		})
		if err != nil {
			for _, session := range sessions {
				_ = session.Close()
			}
			_ = adapter.Disconnect()
			return nil, nil, err
		}
		if err := conn.Connect(); err != nil {
			_ = conn.Close()
			for _, session := range sessions {
				_ = session.Close()
			}
			_ = adapter.Disconnect()
			return nil, nil, fmt.Errorf("connect session %d: %w", index, err)
		}
		sessions = append(sessions, protocol.NewSession(conn))
	}
	return adapter, protocol.NewPool(sessions...), nil
}

func decodePublicKey(value string) (ed25519.PublicKey, error) {
	key, err := hex.DecodeString(value)
	if err != nil || len(key) != ed25519.PublicKeySize {
		return nil, fmt.Errorf("server public key must be %d-byte hex", ed25519.PublicKeySize)
	}
	return ed25519.PublicKey(key), nil
}
