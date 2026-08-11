package main

import (
	"context"
	"errors"
	"log"
	"net"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"

	"github.com/anyshake/telekit/example/p2pssh/common"
)

func init() {
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)
	log.SetPrefix("[telekit p2pssh/client] ")
}

func main() {
	args := parseCliArguments()
	clientID := strings.TrimSpace(args.clientID)

	if args.room == "" {
		log.Fatal("room is required; pass -room <id>")
	}

	if clientID == "" {
		log.Fatal("client-id cannot be empty")
	}

	serverPublicKey, err := decodeServerPublicKey(args.serverPublicKey)
	if err != nil {
		log.Fatalf("decode server public key: %v", err)
	}
	transport, err := common.NewTransport(args.transportName)
	if err != nil {
		log.Fatalln(err)
	}

	listener, err := net.Listen("tcp", args.listenAddr)
	if err != nil {
		log.Fatalf("listen on %s: %v", args.listenAddr, err)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	go func() {
		<-ctx.Done()

		log.Printf("shutdown requested: %v", ctx.Err())

		if err := listener.Close(); err != nil &&
			!errors.Is(err, net.ErrClosed) {
			log.Printf("close listener failed: %v", err)
		}
	}()

	log.Printf(
		"proxy listening on %s (client ID %q)",
		args.listenAddr,
		clientID,
	)

	var wg sync.WaitGroup
	sessionConfig := telekitSessionConfig{
		room:               args.room,
		mqttBroker:         args.mqttBroker,
		baseTopic:          args.baseTopic,
		secret:             args.secret,
		serverPublicKey:    serverPublicKey,
		timeout:            args.timeout,
		compression:        args.compression,
		maxFrameBytes:      args.maxFrameBytes,
		receiveBufferBytes: args.receiveBufferBytes,
		maxPendingICE:      args.maxPendingICE,
		maxPendingICEBytes: args.maxPendingICEBytes,
		transport:          transport,
		queueMessages:      args.queueMessages,
		queueBytes:         args.queueBytes,
	}

	proxy := &telekitProxy{
		cfg:      sessionConfig,
		clientID: clientID,
		done:     make(chan struct{}),
	}
	defer proxy.Close()

	go func() {
		select {
		case <-ctx.Done():
		case <-proxy.Done():
			log.Println("Telekit proxy closed; stopping client")
			proxy.Close()
		}

		if err := listener.Close(); err != nil &&
			!errors.Is(err, net.ErrClosed) {
			log.Printf("close listener failed: %v", err)
		}
	}()

	for {
		localConn, err := listener.Accept()
		if err != nil {
			if ctx.Err() != nil || errors.Is(err, net.ErrClosed) {
				break
			}

			log.Printf("accept failed: %v", err)
			continue
		}

		if ctx.Err() != nil {
			_ = localConn.Close()
			break
		}

		log.Printf("accepted local connection from %s client_id=%q", localConn.RemoteAddr(), clientID)

		wg.Add(1)

		go func(conn net.Conn) {
			defer wg.Done()
			proxyConn(
				ctx,
				conn,
				clientID,
				proxy,
			)
		}(localConn)
	}

	wg.Wait()
	log.Println("client stopped")
}
