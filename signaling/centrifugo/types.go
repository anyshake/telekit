package centrifugo

import (
	"sync"
	"sync/atomic"

	"github.com/alphadose/haxmap"
	"github.com/centrifugal/centrifuge-go"
)

type option func(*config) error

type config struct {
	wsURL       string
	apiURL      string
	apiKey      string
	token       string
	baseChannel string
}

type subscription struct {
	handler func([]byte)
}

type AdapterImpl struct {
	cfg *config

	client *centrifuge.Client

	subs   map[string]*haxmap.Map[int, *subscription]
	mu     sync.Mutex
	nextID atomic.Int64
}
