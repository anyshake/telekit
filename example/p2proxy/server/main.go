package main

import (
	"context"
	"errors"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"

	"github.com/anyshake/telekit/example/p2proxy/common"
	"github.com/anyshake/telekit/peer"
	peerserver "github.com/anyshake/telekit/peer/server"
)

func init() {
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)
	log.SetPrefix("[telekit proxy/server] ")
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
	resolver, err := newDNSResolver(args.dnsServer, args.dnsTimeout)
	if err != nil {
		log.Fatalln(err)
	}
	requests := newRequestPool(args.workers, args.requestQueue, resolver, args.dialTimeout)
	defer requests.Close()

	listener, err := peerserver.NewListener(args.room, adapter, peer.KeyProviderFunc(func(string) ([]byte, error) {
		return append([]byte(nil), key[:]...), nil
	}), &peerserver.Options{
		IdentityKey:    identityKey,
		EncryptionType: encryptionType,
		Transports:     common.ServerTransports(),
	}, common.APIOptions()...)
	if err != nil {
		log.Fatalln(err)
	}
	defer listener.Close()
	log.Printf("ready: room=%s dns=%s workers=%d queue=%d", args.room, args.dnsServer, args.workers, args.requestQueue)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	go func() {
		<-ctx.Done()
		_ = listener.Close()
		requests.Close()
	}()
	for {
		conn, err := listener.Accept()
		if err != nil {
			if !errors.Is(err, net.ErrClosed) {
				log.Printf("accept failed: %v", err)
			}
			return
		}
		go serveTunnel(conn, requests)
	}
}
