package main

import (
	"flag"
	"log"
	"time"

	"github.com/anyshake/telekit/example/p2pssh/common"
	"github.com/anyshake/telekit/utils/compression"
)

type arguments struct {
	room                 string
	mqttBroker           string
	baseTopic            string
	secret               string
	identitySeed         string
	hostKeyPath          string
	username             string
	password             string
	shell                string
	compression          bool
	maxFrameBytes        int
	receiveBufferBytes   int
	maxBufferedBytes     int64
	maxPendingICE        int
	maxPendingICEBytes   int
	maxConnections       int
	maxPendingHandshakes int
	handshakeTimeout     time.Duration
	allowTCPForwarding   bool
	forwardDialTimeout   time.Duration
	helloRate            float64
	helloBurst           int
	queueMessages        int
	queueBytes           int
}

func parseCliArguments() *arguments {
	var args arguments
	flag.StringVar(&args.room, "room", "", "room ID; leave empty to auto-generate one")
	flag.StringVar(&args.mqttBroker, "mqtt", "wss://mqtt-dashboard.com:8884/mqtt", "MQTT broker URL")
	flag.StringVar(&args.baseTopic, "mqtt-base-topic", "telekit", "MQTT base topic")
	flag.StringVar(&args.secret, "secret", "change-me", "pre-shared passphrase accepted for demo clients")
	flag.StringVar(&args.identitySeed, "identity-seed", common.DefaultIdentitySeed, "Ed25519 server identity seed (hex)")
	flag.StringVar(&args.hostKeyPath, "host-key", "./host_key", "path to SSH host key")
	flag.StringVar(&args.username, "username", "root", "login username for SSH server")
	flag.StringVar(&args.password, "password", "passw0rd", "login password for SSH server")
	flag.StringVar(&args.shell, "shell", "/bin/sh", "shell to execute for SSH sessions (*nix only)")
	flag.BoolVar(&args.compression, "compression", true, "enable zstd compression before encryption")
	flag.IntVar(&args.maxFrameBytes, "max-frame-bytes", 4<<20, "maximum encrypted transport frame size")
	flag.IntVar(&args.receiveBufferBytes, "receive-buffer-bytes", 8<<20, "maximum unread stream data per connection")
	flag.Int64Var(&args.maxBufferedBytes, "max-buffered-bytes", 256<<20, "server-wide reassembly, receive, ICE, and callback budget")
	flag.IntVar(&args.maxPendingICE, "max-pending-ice", 128, "maximum queued ICE Candidates per connection")
	flag.IntVar(&args.maxPendingICEBytes, "max-pending-ice-bytes", 256<<10, "maximum queued ICE Candidate bytes per connection")
	flag.IntVar(&args.maxConnections, "max-connections", 1024, "maximum total client connections")
	flag.IntVar(&args.maxPendingHandshakes, "max-pending-handshakes", 256, "maximum authenticated handshakes awaiting DataChannel open")
	flag.DurationVar(&args.handshakeTimeout, "handshake-timeout", 30*time.Second, "maximum lifetime of a pending handshake")
	flag.BoolVar(&args.allowTCPForwarding, "allow-tcp-forwarding", true, "allow SSH direct-tcpip forwarding used by ssh -D and ssh -L")
	flag.DurationVar(&args.forwardDialTimeout, "forward-dial-timeout", 15*time.Second, "timeout for SSH TCP forwarding target connections")
	flag.Float64Var(&args.helloRate, "hello-rate", 100, "ClientHello rate limit per second")
	flag.IntVar(&args.helloBurst, "hello-burst", 200, "ClientHello token-bucket burst")
	flag.IntVar(&args.queueMessages, "mqtt-queue-messages", 1024, "maximum queued MQTT signaling messages")
	flag.IntVar(&args.queueBytes, "mqtt-queue-bytes", 16<<20, "maximum queued MQTT signaling bytes")
	flag.Parse()

	if args.compression && args.maxFrameBytes > compression.MaxDecodedSize {
		log.Printf("compression enabled; capping max-frame-bytes from %d to %d", args.maxFrameBytes, compression.MaxDecodedSize)
		args.maxFrameBytes = compression.MaxDecodedSize
	}

	return &args
}
