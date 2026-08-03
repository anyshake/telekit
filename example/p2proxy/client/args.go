package main

import (
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/anyshake/telekit/example/p2proxy/common"
)

type arguments struct {
	room            string
	mqttBroker      string
	baseTopic       string
	clientID        string
	secret          string
	serverPublicKey string
	timeout         time.Duration
	transportName   string
	poolSize        int
	socksAddr       string
	queueMessages   int
	queueBytes      int
}

func parseCliArguments() *arguments {
	var args arguments
	flag.StringVar(&args.room, "room", "p2proxy-demo", "room ID")
	flag.StringVar(&args.mqttBroker, "mqtt", "wss://mqtt-dashboard.com:8884/mqtt", "MQTT broker URL")
	flag.StringVar(&args.baseTopic, "mqtt-base-topic", "telekit", "MQTT base topic")
	flag.StringVar(&args.clientID, "client-id", fmt.Sprintf("p2proxy-client-%d", os.Getpid()), "unique PSK client identity")
	flag.StringVar(&args.secret, "secret", "change-me", "pre-shared passphrase")
	flag.StringVar(&args.serverPublicKey, "server-public-key", common.DefaultServerPublicKey, "pinned Ed25519 server public key (hex)")
	flag.DurationVar(&args.timeout, "timeout", 30*time.Second, "telekit connection timeout")
	flag.StringVar(&args.transportName, "transport", "quic", "transport selected after ICE (quic, kcp, or sctp)")
	flag.IntVar(&args.poolSize, "pool-size", 2, "number of telekit sessions used for SOCKS requests")
	flag.StringVar(&args.socksAddr, "socks", "127.0.0.1:1080", "local SOCKS5 listen address")
	flag.IntVar(&args.queueMessages, "mqtt-queue-messages", 1024, "maximum queued MQTT signaling messages")
	flag.IntVar(&args.queueBytes, "mqtt-queue-bytes", 16<<20, "maximum queued MQTT signaling bytes")
	flag.Parse()
	return &args
}
