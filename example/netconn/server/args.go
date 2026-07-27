package main

import (
	"flag"
	"time"
)

const demoServerIdentitySeed = "TDkNs3EMNecsKy+t435mMxT9gf232n3t3KnmSOazwXo="

type arguments struct {
	mqttBroker           string
	baseTopic            string
	roomId               string
	preSharedKey         string
	identitySeed         string
	compression          bool
	maxFrameBytes        int
	receiveBufferBytes   int
	maxBufferedBytes     int64
	maxConnections       int
	maxPendingHandshakes int
	handshakeTimeout     time.Duration
	helloRate            float64
	helloBurst           int
	mqttQueueMessages    int
	mqttQueueBytes       int
}

func parseCliArguments() *arguments {
	var args arguments
	flag.StringVar(&args.mqttBroker, "mqtt", "wss://mqtt-dashboard.com:8884/mqtt", "MQTT broker URL")
	flag.StringVar(&args.baseTopic, "mqtt-base-topic", "telekit-base-topic", "MQTT base topic")
	flag.StringVar(&args.roomId, "room", "example-netconn", "room ID")
	flag.StringVar(&args.preSharedKey, "secret", "change@me", "pre-shared passphrase accepted for demo clients")
	flag.StringVar(&args.identitySeed, "identity-seed", demoServerIdentitySeed, "Ed25519 server identity seed (base64)")
	flag.BoolVar(&args.compression, "compression", false, "enable zstd compression before encryption")
	flag.IntVar(&args.maxFrameBytes, "max-frame-bytes", 4<<20, "maximum encrypted transport frame size")
	flag.IntVar(&args.receiveBufferBytes, "receive-buffer-bytes", 8<<20, "maximum unread stream data per connection")
	flag.Int64Var(&args.maxBufferedBytes, "max-buffered-bytes", 256<<20, "server-wide reassembly and receive budget")
	flag.IntVar(&args.maxConnections, "max-connections", 1024, "maximum total client connections")
	flag.IntVar(&args.maxPendingHandshakes, "max-pending-handshakes", 256, "maximum authenticated handshakes awaiting transport establishment")
	flag.DurationVar(&args.handshakeTimeout, "handshake-timeout", 30*time.Second, "maximum lifetime of a pending handshake")
	flag.Float64Var(&args.helloRate, "hello-rate", 100, "ClientHello rate limit per second")
	flag.IntVar(&args.helloBurst, "hello-burst", 200, "ClientHello token-bucket burst")
	flag.IntVar(&args.mqttQueueMessages, "mqtt-queue-messages", 1024, "maximum queued MQTT signaling messages")
	flag.IntVar(&args.mqttQueueBytes, "mqtt-queue-bytes", 16<<20, "maximum queued MQTT signaling bytes")
	flag.Parse()
	return &args
}
