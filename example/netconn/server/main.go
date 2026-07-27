package main

import (
	"context"
	"crypto/sha256"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/anyshake/telekit/peer"
	"github.com/anyshake/telekit/peer/server"
)

func init() {
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)
	log.SetPrefix("[telekit netconn/server] ")
}

func main() {
	args := parseCliArguments()
	log.Printf("startup: room=%q signaling=mqtt base-topic=%q", args.roomId, args.baseTopic)
	log.Printf("limits: frame=%d receive=%d global=%d connections=%d pending=%d/%s hello=%.2f/%d mqtt=%d/%d",
		args.maxFrameBytes, args.receiveBufferBytes, args.maxBufferedBytes,
		args.maxConnections, args.maxPendingHandshakes, args.handshakeTimeout,
		args.helloRate, args.helloBurst, args.mqttQueueMessages, args.mqttQueueBytes)

	adapter, err := connectMQTT(args.mqttBroker, args.baseTopic, "server", args.mqttQueueMessages, args.mqttQueueBytes)
	if err != nil {
		log.Fatalln(err)
	}
	defer adapter.Disconnect()

	identityKey, err := decodeIdentityKey(args.identitySeed)
	if err != nil {
		log.Fatalln(err)
	}
	key := sha256.Sum256([]byte(args.preSharedKey))
	listener, err := server.NewListener(
		args.roomId,
		adapter,
		peer.KeyProviderFunc(func(clientID string) ([]byte, error) {
			log.Printf("handshake: authenticating client=%q", clientID)
			return append([]byte(nil), key[:]...), nil
		}),
		&server.Options{
			IdentityKey:          identityKey,
			Transports:           serverTransports(),
			UseCompression:       args.compression,
			MaxFrameSize:         args.maxFrameBytes,
			ReceiveBufferSize:    args.receiveBufferBytes,
			MaxBufferedBytes:     args.maxBufferedBytes,
			MaxConnections:       args.maxConnections,
			MaxPendingHandshakes: args.maxPendingHandshakes,
			HandshakeTimeout:     args.handshakeTimeout,
			HelloRateLimit:       args.helloRate,
			HelloRateBurst:       args.helloBurst,
		},
		apiOptions()...,
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
