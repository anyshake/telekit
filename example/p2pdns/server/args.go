package main

import (
	"crypto/sha256"
	"flag"
	"time"
)

type arguments struct {
	mqttBroker    string
	baseTopic     string
	room          string
	secret        string
	identitySeed  string
	upstreamDNS   string
	dnsTimeout    time.Duration
	queueMessages int
	queueBytes    int
}

func parseCliArguments() *arguments {
	var args arguments
	flag.StringVar(&args.mqttBroker, "mqtt", "wss://mqtt-dashboard.com:8884/mqtt", "MQTT broker URL")
	flag.StringVar(&args.baseTopic, "mqtt-base-topic", "telekit", "MQTT base topic")
	flag.StringVar(&args.room, "room", "p2pdns", "room ID")
	flag.StringVar(&args.secret, "secret", "change-me", "pre-shared passphrase")
	flag.StringVar(&args.identitySeed, "identity-seed", defaultIdentitySeed, "Ed25519 server identity seed (hex)")
	flag.StringVar(&args.upstreamDNS, "upstream", "1.1.1.1:53", "upstream DNS UDP address")
	flag.DurationVar(&args.dnsTimeout, "dns-timeout", 5*time.Second, "upstream DNS response timeout")
	flag.IntVar(&args.queueMessages, "mqtt-queue-messages", 1024, "maximum queued MQTT signaling messages")
	flag.IntVar(&args.queueBytes, "mqtt-queue-bytes", 16<<20, "maximum queued MQTT signaling bytes")
	flag.Parse()
	return &args
}

func keyFromSecret(secret string) [sha256.Size]byte {
	return sha256.Sum256([]byte(secret))
}
