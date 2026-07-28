package nats

import (
	"sync"

	natsgo "github.com/nats-io/nats.go"
)

type Adapter struct {
	url         string
	baseSubject string
	opts        []natsgo.Option

	mu sync.RWMutex
	nc *natsgo.Conn
}

const DefaultBaseSubject = "telekit"
