package websocket

import (
	"errors"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/anyshake/telekit/signaling"
	gorilla "github.com/gorilla/websocket"
)

func NewAdapter(baseURL string, opts ...Option) (signaling.Adapter, error) {
	u, err := url.Parse(baseURL)
	if err != nil {
		return nil, err
	}
	if u.Scheme != "ws" && u.Scheme != "wss" {
		return nil, errors.New("WebSocket URL must use ws or wss")
	}
	a := &Adapter{
		baseURL: strings.TrimRight(baseURL, "/"),
		dialer:  gorilla.DefaultDialer,
		headers: make(http.Header),
		rooms:   make(map[string]*roomConn),
	}
	for _, opt := range opts {
		opt(a)
	}
	return a, nil
}

func WithDialer(dialer *gorilla.Dialer) Option {
	return func(a *Adapter) {
		if dialer != nil {
			a.dialer = dialer
		}
	}
}

// WithHTTPHeader supplies credentials or other metadata to the Broker's
// authorization callback. The header map is cloned by NewAdapter.
func WithHTTPHeader(headers http.Header) Option {
	return func(a *Adapter) {
		a.headers = headers.Clone()
	}
}

// WithReconnectBackoff configures the minimum and maximum delay between
// WebSocket reconnect attempts.
func WithReconnectBackoff(minDelay, maxDelay time.Duration) Option {
	return func(a *Adapter) {
		if minDelay > 0 {
			a.reconnectMin = minDelay
		}
		if maxDelay > 0 {
			a.reconnectMax = maxDelay
		}
		if a.reconnectMax > 0 && a.reconnectMax < a.reconnectMin {
			a.reconnectMax = a.reconnectMin
		}
	}
}

// WithOnConnect is called when a room WebSocket is connected, including after
// an automatic reconnect.
func WithOnConnect(handler func()) Option {
	return func(a *Adapter) { a.onConnect = handler }
}

// WithConnectionLostHandler is called when a room WebSocket unexpectedly
// closes. Explicit Disconnect does not invoke it.
func WithConnectionLostHandler(handler func(error)) Option {
	return func(a *Adapter) { a.onConnectionLost = handler }
}

// WithReconnectingHandler is called before the adapter starts reconnecting a
// room WebSocket.
func WithReconnectingHandler(handler func()) Option {
	return func(a *Adapter) { a.onReconnecting = handler }
}
