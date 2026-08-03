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

	"github.com/anyshake/telekit/example/wsrelay/common"
	"github.com/anyshake/telekit/peer"
	peerapi "github.com/anyshake/telekit/peer/api"
	"github.com/anyshake/telekit/peer/client"
	"github.com/pion/ice/v4"
)

func init() {
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)
	log.SetPrefix("[telekit wsrelay/client] ")
}

func main() {
	args := parseCLIArguments()

	adapter, err := common.ConnectMQTT(args.mqttBroker, args.baseTopic, "client")
	if err != nil {
		log.Fatalln(err)
	}
	defer adapter.Disconnect()

	serverPublicKey, err := decodeServerPublicKey(args.serverPubKey)
	if err != nil {
		log.Fatalln(err)
	}
	serverID, err := peer.ServerIDFromPublicKey(serverPublicKey)
	if err != nil {
		log.Fatalln(err)
	}

	peerAPI, err := peerapi.NewAPI(
		args.roomID, adapter,
		peerapi.WithWebSocketRelayServer(args.relayBaseURL, serverID, args.relayToken),
	)
	if err != nil {
		log.Fatalln(err)
	}

	log.Printf("startup: room=%q transport=%s relay=%s", args.roomID, args.transport, args.relayBaseURL)
	selectedTransport, err := common.CreateTransport(args.transport)
	if err != nil {
		log.Fatalln(err)
	}

	key := sha256.Sum256([]byte(args.preSharedKey))
	disconnected := make(chan struct{}, 1)
	conn, err := client.NewClient(
		peer.PreSharedKey{ClientID: args.clientID, Key: key[:], ServerPublicKey: serverPublicKey},
		peerAPI,
		&client.Options{
			// always choose relay server
			ICEAgentOptions: []ice.AgentOption{
				ice.WithCandidateTypes([]ice.CandidateType{
					ice.CandidateTypeRelay,
				}),
			},
			Timeout:            args.timeout,
			Transport:          selectedTransport,
			UseCompression:     args.compression,
			MaxFrameSize:       args.maxFrameBytes,
			ReceiveBufferSize:  args.receiveBufferSize,
			OnClientHello:      handleClientHello,
			OnServerHello:      handleServerHello,
			OnConnectionFailed: handleConnectionFailed,
			OnDisconnected:     handleDisconnected(disconnected),
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
		case <-disconnected:
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
		log.Printf("stdin: %v", err)
	}
}
