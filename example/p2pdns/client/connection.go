package main

import (
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"net"

	"github.com/anyshake/telekit/example/p2pdns/common"
	"github.com/anyshake/telekit/peer"
	peerapi "github.com/anyshake/telekit/peer/api"
	peerclient "github.com/anyshake/telekit/peer/client"
	"github.com/anyshake/telekit/signaling"
	transportrawudp "github.com/anyshake/telekit/transports/transport_rawudp"
	"github.com/anyshake/telekit/utils/encryption"
)

func connectTelekit(args *arguments) (signaling.Adapter, *peerclient.Client, net.Conn, error) {
	serverPublicKey, err := decodePublicKey(args.serverPublicKey)
	if err != nil {
		return nil, nil, nil, err
	}
	adapter, err := common.ConnectMQTT(args.mqttBroker, args.baseTopic, "client", args.queueMessages, args.queueBytes)
	if err != nil {
		return nil, nil, nil, err
	}
	api, err := peerapi.NewAPI(args.room, adapter, common.APIOptions()...)
	if err != nil {
		_ = adapter.Disconnect()
		return nil, nil, nil, err
	}
	key := sha256.Sum256([]byte(args.secret))
	conn, err := peerclient.NewClient(peer.PreSharedKey{
		ClientID:        args.clientID,
		Key:             key[:],
		ServerPublicKey: serverPublicKey,
	}, api, &peerclient.Options{
		Timeout:        args.timeout,
		Transport:      transportrawudp.New(),
		EncryptionType: encryption.AES_256_GCM,
	})
	if err != nil {
		_ = adapter.Disconnect()
		return nil, nil, nil, err
	}
	if err := conn.Connect(); err != nil {
		_ = conn.Close()
		_ = adapter.Disconnect()
		return nil, nil, nil, err
	}
	return adapter, conn, conn, nil
}

func decodePublicKey(value string) (ed25519.PublicKey, error) {
	key, err := hex.DecodeString(value)
	if err != nil || len(key) != ed25519.PublicKeySize {
		return nil, fmt.Errorf("server public key must be %d-byte hex", ed25519.PublicKeySize)
	}
	return ed25519.PublicKey(key), nil
}
