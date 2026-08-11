package main

import (
	"crypto/ed25519"
	"time"

	"github.com/anyshake/telekit/transports"
)

type telekitSessionConfig struct {
	room               string
	mqttBroker         string
	baseTopic          string
	secret             string
	serverPublicKey    ed25519.PublicKey
	timeout            time.Duration
	maxFrameBytes      int
	receiveBufferBytes int
	maxPendingICE      int
	maxPendingICEBytes int
	queueMessages      int
	queueBytes         int
	transport          transports.ITransport
}
