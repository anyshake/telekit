package main

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"

	"github.com/anyshake/telekit/example/p2pssh/common"
	"github.com/anyshake/telekit/peer"
	peerserver "github.com/anyshake/telekit/peer/server"
	"golang.org/x/crypto/ssh"
)

func init() {
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)
	log.SetPrefix("[telekit p2pssh/server] ")
}

func main() {
	args := parseCliArguments()

	if args.room == "" {
		buf := make([]byte, 16)
		if _, err := rand.Read(buf); err != nil {
			log.Fatal(err)
		}
		args.room = hex.EncodeToString(buf)
	}

	signer, err := loadOrCreateHostKey(args.hostKeyPath)
	if err != nil {
		log.Fatal(err)
	}

	sshConfig := &ssh.ServerConfig{
		PasswordCallback: func(
			conn ssh.ConnMetadata,
			password []byte,
		) (*ssh.Permissions, error) {
			if conn.User() == args.username &&
				string(password) == args.password {
				return nil, nil
			}

			return nil, fmt.Errorf(
				"password rejected for %q",
				conn.User(),
			)
		},
	}

	sshConfig.AddHostKey(signer)

	adapter, err := common.ConnectMQTT(
		args.mqttBroker,
		args.baseTopic,
		"server",
		args.queueMessages,
		args.queueBytes,
	)
	if err != nil {
		log.Fatal(err)
	}
	defer adapter.Disconnect()

	key := sha256.Sum256([]byte(args.secret))

	identityKey, err := decodeIdentityKey(args.identitySeed)
	if err != nil {
		log.Fatal(err)
	}

	listener, err := peerserver.NewListener(
		args.room,
		adapter,
		peer.KeyProviderFunc(
			setupKeyProvider(key[:]),
		),
		&peerserver.Options{
			IdentityKey:          identityKey,
			UseCompression:       args.compression,
			MaxFrameSize:         args.maxFrameBytes,
			ReceiveBufferSize:    args.receiveBufferBytes,
			MaxBufferedBytes:     args.maxBufferedBytes,
			MaxPendingICE:        args.maxPendingICE,
			MaxPendingICEBytes:   args.maxPendingICEBytes,
			MaxConnections:       args.maxConnections,
			MaxPendingHandshakes: args.maxPendingHandshakes,
			HandshakeTimeout:     args.handshakeTimeout,
			HelloRateLimit:       args.helloRate,
			HelloRateBurst:       args.helloBurst,
			Transports:           common.ServerTransports(),
		},
		common.APIOptions()...,
	)
	if err != nil {
		log.Fatal(err)
	}
	defer listener.Close()

	log.Printf("room: %s", args.room)
	log.Printf(
		"ready: room=%q signaling=mqtt base-topic=%q tcp_forwarding=%t",
		args.room,
		args.baseTopic,
		args.allowTCPForwarding,
	)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	go acceptLoop(
		listener,
		sshConfig,
		args.shell,
		args.allowTCPForwarding,
		args.forwardDialTimeout,
	)

	<-ctx.Done()

	log.Printf("shutdown requested: %v", ctx.Err())

	if err := listener.Close(); err != nil &&
		!errors.Is(err, net.ErrClosed) {
		log.Printf("close listener failed: %v", err)
	}
}
