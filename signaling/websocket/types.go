package websocket

import (
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/anyshake/telekit/signaling"
	gorilla "github.com/gorilla/websocket"
)

type packet struct {
	Type    signaling.MessageType `json:"type"`
	Payload []byte                `json:"payload"`
}

type roomConn struct {
	conn *gorilla.Conn

	writeMu   sync.Mutex
	closeOnce sync.Once
	done      chan struct{}
	handlerM  sync.RWMutex
	handlers  map[signaling.MessageType]map[uint64]signaling.Handler
}

type Adapter struct {
	baseURL string
	dialer  *gorilla.Dialer
	headers http.Header

	mu               sync.Mutex
	connected        bool
	rooms            map[string]*roomConn
	nextID           atomic.Uint64
	reconnectMin     time.Duration
	reconnectMax     time.Duration
	onConnect        func()
	onConnectionLost func(error)
	onReconnecting   func()
}

const (
	websocketReconnectMin = 250 * time.Millisecond
	websocketReconnectMax = 30 * time.Second
)

type Option func(*Adapter)
