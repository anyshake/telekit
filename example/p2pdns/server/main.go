package main

import (
	"context"
	"errors"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"

	"github.com/anyshake/telekit/example/p2pdns/common"
	"github.com/anyshake/telekit/peer"
	peerserver "github.com/anyshake/telekit/peer/server"
	transportrawudp "github.com/anyshake/telekit/transports/transport_rawudp"
	"github.com/anyshake/telekit/utils/encryption"
)

func init() {
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)
	log.SetPrefix("[telekit p2pdns/server] ")
}

func main() {
	args := parseCliArguments()
	identityKey, err := decodeIdentityKey(args.identitySeed)
	if err != nil {
		log.Fatalln(err)
	}
	key := keyFromSecret(args.secret)
	adapter, err := common.ConnectMQTT(args.mqttBroker, args.baseTopic, "server", args.queueMessages, args.queueBytes)
	if err != nil {
		log.Fatalln(err)
	}
	defer adapter.Disconnect()
	forwarder, err := newDNSForwarder(args.upstreamDNS, args.dnsTimeout)
	if err != nil {
		log.Fatalln(err)
	}
	defer forwarder.Close()
	go forwarder.Run()

	listener, err := peerserver.NewListener(args.room, adapter, peer.KeyProviderFunc(func(string) ([]byte, error) {
		return append([]byte(nil), key[:]...), nil
	}), &peerserver.Options{
		IdentityKey:    identityKey,
		EncryptionType: encryption.AES_256_GCM,
		Transport:      transportrawudp.New(),
	}, common.APIOptions()...)
	if err != nil {
		log.Fatalln(err)
	}
	defer listener.Close()
	log.Printf("ready: room=%s upstream=%s transport=raw_udp", args.room, args.upstreamDNS)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	go func() {
		<-ctx.Done()
		_ = listener.Close()
		_ = forwarder.Close()
	}()
	for {
		conn, err := listener.Accept()
		if err != nil {
			if !errors.Is(err, net.ErrClosed) {
				log.Printf("accept failed: %v", err)
			}
			return
		}
		go serveClient(conn, forwarder)
	}
}
