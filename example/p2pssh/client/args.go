package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/anyshake/telekit/example/p2pssh/common"
	"github.com/anyshake/telekit/utils/compression"
)

type arguments struct {
	room               string
	mqttBroker         string
	baseTopic          string
	clientID           string
	secret             string
	serverPublicKey    string
	timeout            time.Duration
	listenAddr         string
	transportName      string
	compression        bool
	maxFrameBytes      int
	receiveBufferBytes int
	maxPendingICE      int
	maxPendingICEBytes int
	queueMessages      int
	queueBytes         int
}

func parseCliArguments() *arguments {
	var args arguments
	flag.StringVar(&args.room, "room", "", "room ID (required)")
	flag.StringVar(&args.mqttBroker, "mqtt", "wss://mqtt-dashboard.com:8884/mqtt", "MQTT broker URL")
	flag.StringVar(&args.baseTopic, "mqtt-base-topic", "telekit", "MQTT base topic")
	flag.StringVar(&args.clientID, "client-id", fmt.Sprintf("p2pssh-client-%d", os.Getpid()), "unique PSK client identity")
	flag.StringVar(&args.secret, "secret", "change-me", "pre-shared passphrase")
	flag.StringVar(&args.serverPublicKey, "server-public-key", common.DefaultServerPublicKey, "pinned Ed25519 server public key (hex)")
	flag.DurationVar(&args.timeout, "timeout", 30*time.Second, "telekit connection timeout")
	flag.StringVar(&args.listenAddr, "listen", "127.0.0.1:2222", "local SSH/SFTP listen address")
	flag.StringVar(&args.transportName, "transport", "quic", "transport selected after ICE (quic, kcp, sctp, or raknet)")
	flag.BoolVar(&args.compression, "compression", true, "enable zstd compression before encryption")
	flag.IntVar(&args.maxFrameBytes, "max-frame-bytes", 4<<20, "maximum encrypted transport frame size")
	flag.IntVar(&args.receiveBufferBytes, "receive-buffer-bytes", 8<<20, "maximum unread stream data")
	flag.IntVar(&args.maxPendingICE, "max-pending-ice", 128, "maximum queued ICE Candidates")
	flag.IntVar(&args.maxPendingICEBytes, "max-pending-ice-bytes", 256<<10, "maximum queued ICE Candidate bytes")
	flag.IntVar(&args.queueMessages, "mqtt-queue-messages", 1024, "maximum queued MQTT signaling messages")
	flag.IntVar(&args.queueBytes, "mqtt-queue-bytes", 16<<20, "maximum queued MQTT signaling bytes")
	flag.Parse()

	if args.compression && args.maxFrameBytes > compression.MaxDecodedSize {
		log.Printf("compression enabled; capping max-frame-bytes from %d to %d", args.maxFrameBytes, compression.MaxDecodedSize)
		args.maxFrameBytes = compression.MaxDecodedSize
	}

	return &args
}
