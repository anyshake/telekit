package main

import (
	"crypto/sha256"
	"flag"
	"time"

	"github.com/anyshake/telekit/example/p2proxy/common"
)

type arguments struct {
	mqttBroker    string
	baseTopic     string
	room          string
	secret        string
	identitySeed  string
	dialTimeout   time.Duration
	dnsServer     string
	dnsTimeout    time.Duration
	workers       int
	requestQueue  int
	queueMessages int
	queueBytes    int
}

func parseCliArguments() *arguments {
	var args arguments
	flag.StringVar(&args.mqttBroker, "mqtt", "wss://mqtt-dashboard.com:8884/mqtt", "MQTT broker URL")
	flag.StringVar(&args.baseTopic, "mqtt-base-topic", "telekit", "MQTT base topic")
	flag.StringVar(&args.room, "room", "p2proxy-demo", "room ID")
	flag.StringVar(&args.secret, "secret", "change-me", "pre-shared passphrase")
	flag.StringVar(&args.identitySeed, "identity-seed", common.DefaultIdentitySeed, "Ed25519 server identity seed (hex)")
	flag.DurationVar(&args.dialTimeout, "dial-timeout", 15*time.Second, "target TCP dial timeout")
	flag.StringVar(&args.dnsServer, "dns", "1.1.1.1:53", "upstream DNS address (UDP)")
	flag.DurationVar(&args.dnsTimeout, "dns-timeout", 3*time.Second, "timeout for upstream DNS querying")
	flag.IntVar(&args.workers, "workers", 128, "proxy target request worker count")
	flag.IntVar(&args.requestQueue, "request-queue", 1024, "maximum queued proxy target requests")
	flag.IntVar(&args.queueMessages, "mqtt-queue-messages", 1024, "maximum queued MQTT signaling messages")
	flag.IntVar(&args.queueBytes, "mqtt-queue-bytes", 16<<20, "maximum queued MQTT signaling bytes")
	flag.Parse()
	return &args
}

func keyFromSecret(secret string) [sha256.Size]byte {
	return sha256.Sum256([]byte(secret))
}
