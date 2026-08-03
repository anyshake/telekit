package main

import (
	"flag"
	"time"
)

const demoServerIdentitySeed = "4c390db3710c35e72c2b2fade37e663314fd81fdb7da7deddca9e648e6b3c17a"

type arguments struct {
	mqttBroker           string
	baseTopic            string
	roomID               string
	preSharedKey         string
	identitySeed         string
	compression          bool
	maxFrameBytes        int
	receiveBufferSize    int
	maxBufferedBytes     int64
	maxConnections       int
	maxPendingHandshakes int
	handshakeTimeout     time.Duration
	helloRate            float64
	helloBurst           int
	relayBaseURL         string
	relayToken           string
}

func parseCLIArguments() *arguments {
	args := &arguments{}
	flag.StringVar(&args.mqttBroker, "mqtt", "wss://mqtt-dashboard.com:8884/mqtt", "MQTT broker URL")
	flag.StringVar(&args.baseTopic, "mqtt-base-topic", "telekit-wsrelay", "MQTT base topic")
	flag.StringVar(&args.roomID, "room", "example-wsrelay", "room ID")
	flag.StringVar(&args.preSharedKey, "secret", "change@me", "pre-shared passphrase accepted for demo clients")
	flag.StringVar(&args.identitySeed, "identity-seed", demoServerIdentitySeed, "Ed25519 server identity seed (hex)")
	flag.BoolVar(&args.compression, "compression", false, "enable zstd compression")
	flag.IntVar(&args.maxFrameBytes, "max-frame-bytes", 4<<20, "maximum encrypted transport frame size")
	flag.IntVar(&args.receiveBufferSize, "receive-buffer-bytes", 8<<20, "maximum unread stream data per connection")
	flag.Int64Var(&args.maxBufferedBytes, "max-buffered-bytes", 256<<20, "server-wide receive and frame-reassembly budget")
	flag.IntVar(&args.maxConnections, "max-connections", 1024, "maximum client connections")
	flag.IntVar(&args.maxPendingHandshakes, "max-pending-handshakes", 256, "maximum pending handshakes")
	flag.DurationVar(&args.handshakeTimeout, "handshake-timeout", 30*time.Second, "pending handshake timeout")
	flag.Float64Var(&args.helloRate, "hello-rate", 100, "ClientHello rate per second")
	flag.IntVar(&args.helloBurst, "hello-burst", 200, "ClientHello burst")
	flag.StringVar(&args.relayBaseURL, "relay-base-url", "ws://127.0.0.1:8080", "WebSocket relay URL; empty path uses /relay/<server-id>, trailing slash appends server ID")
	flag.StringVar(&args.relayToken, "relay-token", "change@me", "WebSocket relay authorization token")
	flag.Parse()
	return args
}
