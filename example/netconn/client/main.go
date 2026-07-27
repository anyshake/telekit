package main

import (
	"bufio"
	"context"
	"crypto/sha256"
	"fmt"
	"io"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/anyshake/telekit/peer"
	peerapi "github.com/anyshake/telekit/peer/api"
	"github.com/anyshake/telekit/peer/client"
)

func init() {
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)
	log.SetPrefix("[telekit netconn/client] ")
}

func main() {
	args := parseCliArguments()
	log.Printf("startup: room=%q signaling=mqtt base-topic=%q transport=%s", args.roomId, args.baseTopic, args.transport)
	log.Printf("limits: max-frame=%d max-unread=%d ", args.maxFrameBytes, args.recvBufferBytes)

	signalingAdapter, err := connectMQTT(
		args.mqttBroker,
		args.baseTopic,
		"client",
		args.mqttQueueMessages,
		args.mqttQueueBytes,
	)
	if err != nil {
		log.Fatalln(err)
	}
	defer signalingAdapter.Disconnect()

	peerApi, err := peerapi.NewAPI(
		args.roomId,
		signalingAdapter,
		peerapi.WithSTUNServer(
			"stun://stun.cloudflare.com:3478",
			"stun://global.stun.twilio.com:3478",
			"stun://stun.l.google.com:19302",
		),
	)
	if err != nil {
		log.Fatalln(err)
	}

	serverPublicKey, err := decodeServerPublicKey(args.serverPubKey)
	if err != nil {
		log.Fatalln(err)
	}

	selectedTransport, err := createTransport(args.transport)
	if err != nil {
		log.Fatalln(err)
	}

	preSharedKey := sha256.Sum256([]byte(args.preSharedKey))
	conn, err := client.NewClient(
		peer.PreSharedKey{
			ClientID:        args.clientId,
			Key:             preSharedKey[:],
			ServerPublicKey: serverPublicKey,
		},
		peerApi,
		&client.Options{
			Timeout:            args.timeout,
			Transport:          selectedTransport,
			UseCompression:     args.compression,
			MaxFrameSize:       args.maxFrameBytes,
			ReceiveBufferSize:  args.recvBufferBytes,
			OnClientHello:      handleClientHello,
			OnServerHello:      handleServerHello,
			OnConnectionFailed: handleConnectionFailed,
			OnDisconnected:     handleDisconnect,
		},
	)
	if err != nil {
		log.Fatalln(err)
	}
	defer conn.Close()
	if err := conn.Connect(); err != nil {
		log.Fatalln(err)
	}
	log.Printf("connection: opened local=%s remote=%s", conn.LocalAddr(), conn.RemoteAddr())

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	go func() {
		<-ctx.Done()
		_ = conn.Close()
	}()
	lines := make(chan string)
	go scanLines(lines)
	reader := bufio.NewReader(conn)
	log.Println("ready: type a line and press Enter; Ctrl-D or Ctrl-C exits")
	for {
		fmt.Print("> ")
		select {
		case <-ctx.Done():
			return
		case line, ok := <-lines:
			if !ok {
				return
			}
			if _, err := fmt.Fprintln(conn, line); err != nil {
				log.Printf("data: write failed: %v", err)
				return
			}
			echo, err := reader.ReadString('\n')
			if err != nil {
				if err != io.EOF {
					log.Printf("data: read failed: %v", err)
				}
				return
			}
			fmt.Printf("server> %s", echo)
		}
	}
}

func scanLines(lines chan<- string) {
	defer close(lines)

	scanner := bufio.NewScanner(os.Stdin)
	for scanner.Scan() {
		lines <- scanner.Text()
	}

	if err := scanner.Err(); err != nil {
		log.Printf("failed to read stdin: %v", err)
	}
}
