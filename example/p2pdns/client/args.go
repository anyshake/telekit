package main

import (
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/anyshake/telekit/example/p2pdns/common"
)

type arguments struct {
	room            string
	mqttBroker      string
	baseTopic       string
	clientID        string
	secret          string
	serverPublicKey string
	timeout         time.Duration
	localDNS        string
	queueMessages   int
	queueBytes      int
}

func parseCliArguments() *arguments {
	var args arguments
	flag.StringVar(&args.room, "room", "p2pdns", "room ID")
	flag.StringVar(&args.mqttBroker, "mqtt", "wss://mqtt-dashboard.com:8884/mqtt", "MQTT broker URL")
	flag.StringVar(&args.baseTopic, "mqtt-base-topic", "telekit", "MQTT base topic")
	flag.StringVar(&args.clientID, "client-id", fmt.Sprintf("p2pdns-client-%d", os.Getpid()), "unique PSK client identity")
	flag.StringVar(&args.secret, "secret", "change-me", "pre-shared passphrase")
	flag.StringVar(&args.serverPublicKey, "server-public-key", common.DefaultServerPublicKey, "pinned Ed25519 server public key (hex)")
	flag.DurationVar(&args.timeout, "timeout", 30*time.Second, "telekit connection timeout")
	flag.StringVar(&args.localDNS, "listen", "127.0.0.1:53", "local UDP DNS listen address")
	flag.IntVar(&args.queueMessages, "mqtt-queue-messages", 1024, "maximum queued MQTT signaling messages")
	flag.IntVar(&args.queueBytes, "mqtt-queue-bytes", 16<<20, "maximum queued MQTT signaling bytes")
	flag.Parse()
	return &args
}
