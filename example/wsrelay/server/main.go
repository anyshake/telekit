package main

import (
	"context"
	"crypto/sha256"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/anyshake/telekit/example/wsrelay/common"
	"github.com/anyshake/telekit/peer"
	peerapi "github.com/anyshake/telekit/peer/api"
	"github.com/anyshake/telekit/peer/server"
	"github.com/pion/ice/v4"
)

func init() {
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)
	log.SetPrefix("[telekit wsrelay/server] ")
}

func main() {
	args := parseCLIArguments()

	adapter, err := common.ConnectMQTT(args.mqttBroker, args.baseTopic, "server")
	if err != nil {
		log.Fatalln(err)
	}
	defer adapter.Disconnect()

	identityKey, err := decodeIdentityKey(args.identitySeed)
	if err != nil {
		log.Fatalln(err)
	}
	serverID, err := peer.ServerIDFromPublicKey(identityKey.Public())
	if err != nil {
		log.Fatalln(err)
	}
	log.Printf("startup: room=%q relay=%s", args.roomID, args.relayBaseURL)
	log.Printf("relay server endpoint ID: %s", serverID)
	key := sha256.Sum256([]byte(args.preSharedKey))

	listener, err := server.NewListener(
		args.roomID,
		adapter,
		peer.KeyProviderFunc(func(clientID string) ([]byte, error) {
			log.Printf("handshake: authenticating client=%q", clientID)
			return append([]byte(nil), key[:]...), nil
		}),
		&server.Options{
			// always choose relay server
			ICEAgentOptions: []ice.AgentOption{
				ice.WithCandidateTypes([]ice.CandidateType{
					ice.CandidateTypeRelay,
				}),
			},
			IdentityKey:          identityKey,
			Transports:           common.ServerTransports(),
			UseCompression:       args.compression,
			MaxFrameSize:         args.maxFrameBytes,
			ReceiveBufferSize:    args.receiveBufferSize,
			MaxBufferedBytes:     args.maxBufferedBytes,
			MaxConnections:       args.maxConnections,
			MaxPendingHandshakes: args.maxPendingHandshakes,
			HandshakeTimeout:     args.handshakeTimeout,
			HelloRateLimit:       args.helloRate,
			HelloRateBurst:       args.helloBurst,
		},
		peerapi.WithWebSocketRelayServer(args.relayBaseURL, serverID, args.relayToken),
	)
	if err != nil {
		log.Fatalln(err)
	}
	defer listener.Close()
	log.Printf("ready: listening addr=%s", listener.Addr())

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	go acceptLoop(listener)
	<-ctx.Done()
}
