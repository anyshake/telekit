package main

import (
	"flag"
	"time"
)

const demoServerPublicKey = "2dc55c63afa1d2ca5d958acf19dafbbf3f77b7752a5204e8ceb881d1cc1b7643"

type arguments struct {
	mqttBroker        string
	baseTopic         string
	roomID            string
	clientID          string
	preSharedKey      string
	serverPubKey      string
	timeout           time.Duration
	transport         string
	compression       bool
	maxFrameBytes     int
	receiveBufferSize int
	relayBaseURL      string
	relayToken        string
}

func parseCLIArguments() *arguments {
	args := &arguments{}
	flag.StringVar(&args.mqttBroker, "mqtt", "wss://mqtt-dashboard.com:8884/mqtt", "MQTT broker URL")
	flag.StringVar(&args.baseTopic, "mqtt-base-topic", "telekit-wsrelay", "MQTT base topic")
	flag.StringVar(&args.roomID, "room", "example-wsrelay", "room ID")
	flag.StringVar(&args.clientID, "client-id", "client-a", "unique client identity; must be authorized by the relay")
	flag.StringVar(&args.preSharedKey, "secret", "change@me", "Telekit pre-shared passphrase")
	flag.StringVar(&args.serverPubKey, "server-public-key", demoServerPublicKey, "pinned Ed25519 server public key (hex)")
	flag.DurationVar(&args.timeout, "timeout", 30*time.Second, "connection timeout")
	flag.StringVar(&args.transport, "transport", "quic", "data transport (quic, kcp, sctp, rawudp)")
	flag.BoolVar(&args.compression, "compression", false, "enable zstd compression")
	flag.IntVar(&args.maxFrameBytes, "max-frame-bytes", 4<<20, "maximum encrypted transport frame size")
	flag.IntVar(&args.receiveBufferSize, "receive-buffer-bytes", 8<<20, "maximum unread stream data")
	flag.StringVar(&args.relayBaseURL, "relay-base-url", "ws://127.0.0.1:8080", "WebSocket relay URL; empty path uses /relay/<server-id>, trailing slash appends server ID")
	flag.StringVar(&args.relayToken, "relay-token", "change@me", "WebSocket relay authorization token")
	flag.Parse()
	return args
}
