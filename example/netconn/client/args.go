package main

import (
	"crypto/md5"
	"flag"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/shirou/gopsutil/v4/host"
)

const demoServerPublicKey = "2dc55c63afa1d2ca5d958acf19dafbbf3f77b7752a5204e8ceb881d1cc1b7643"

type arguments struct {
	mqttBroker        string
	baseTopic         string
	roomId            string
	clientId          string
	preSharedKey      string
	serverPubKey      string
	timeout           time.Duration
	transport         string
	compression       bool
	maxFrameBytes     int
	recvBufferBytes   int
	mqttQueueMessages int
	mqttQueueBytes    int
}

func parseCliArguments() *arguments {
	hostId, err := host.HostID()
	if err != nil || hostId == "" {
		hostId = fmt.Sprintf("%x", md5.Sum([]byte(uuid.New().String())))
	} else {
		hostId = fmt.Sprintf("%x", md5.Sum([]byte(hostId)))
	}

	var args arguments
	flag.StringVar(&args.mqttBroker, "mqtt", "wss://mqtt-dashboard.com:8884/mqtt", "MQTT broker URL")
	flag.StringVar(&args.mqttBroker, "mqtt-broker", "wss://mqtt-dashboard.com:8884/mqtt", "alias for -mqtt")
	flag.StringVar(&args.baseTopic, "mqtt-base-topic", "telekit-base-topic", "MQTT base topic")
	flag.StringVar(&args.baseTopic, "base-topic", "telekit-base-topic", "alias for -mqtt-base-topic")
	flag.StringVar(&args.roomId, "room", "example-netconn", "Room ID")
	flag.StringVar(&args.roomId, "room-id", "example-netconn", "alias for -room")
	flag.StringVar(&args.clientId, "client-id", fmt.Sprintf("client-%s", hostId), "Unique client identity")
	flag.StringVar(&args.preSharedKey, "secret", "change@me", "pre-shared passphrase")
	flag.StringVar(&args.preSharedKey, "pre-shared-key", "change@me", "alias for -secret")
	flag.StringVar(&args.serverPubKey, "server-public-key", demoServerPublicKey, "pinned Ed25519 server public key (hex)")
	flag.StringVar(&args.serverPubKey, "server-pub-key", demoServerPublicKey, "alias for -server-public-key")
	flag.DurationVar(&args.timeout, "timeout", 30*time.Second, "connection timeout")
	flag.StringVar(&args.transport, "transport", "quic", "data transport selected after ICE (quic, http3, kcp, sctp, or rawudp)")
	flag.BoolVar(&args.compression, "compression", false, "enable zstd compression before encryption")
	flag.IntVar(&args.maxFrameBytes, "max-frame-bytes", 4<<20, "maximum encrypted transport frame size")
	flag.IntVar(&args.recvBufferBytes, "receive-buffer-bytes", 8<<20, "maximum unread stream data")
	flag.IntVar(&args.recvBufferBytes, "recv-buffer-bytes", 8<<20, "alias for -receive-buffer-bytes")
	flag.IntVar(&args.mqttQueueMessages, "mqtt-queue-messages", 1024, "maximum queued MQTT signaling messages")
	flag.IntVar(&args.mqttQueueBytes, "mqtt-queue-bytes", 16<<20, "maximum queued MQTT signaling bytes")
	flag.Parse()

	return &args
}
