package centrifugo

import (
	"errors"
	"fmt"
	"time"

	"github.com/alphadose/haxmap"
	"github.com/anyshake/telekit/signaling"
	"github.com/centrifugal/centrifuge-go"
)

const DefaultBaseChannel = "telekit"

func NewAdapter(url string, opts ...option) (signaling.IAdapter, error) {
	cfg := &config{wsURL: url, baseChannel: DefaultBaseChannel}
	for _, opt := range opts {
		if err := opt(cfg); err != nil {
			return nil, err
		}
	}

	if cfg.wsURL == "" {
		return nil, errors.New("wsURL is required")
	}

	adapter := &AdapterImpl{
		cfg:  cfg,
		subs: make(map[string]*haxmap.Map[int, *subscription]),
	}
	client := centrifuge.NewJsonClient(
		cfg.wsURL,
		centrifuge.Config{
			Token:             cfg.token,
			MinReconnectDelay: cfg.reconnectMin,
			MaxReconnectDelay: cfg.reconnectMax,
		},
	)
	client.OnConnected(func(centrifuge.ConnectedEvent) {
		adapter.connected.Store(true)
		if cfg.onConnect != nil {
			cfg.onConnect()
		}
	})
	client.OnConnecting(func(event centrifuge.ConnectingEvent) {
		if adapter.connected.Load() {
			if cfg.onConnectionLost != nil {
				cfg.onConnectionLost(fmt.Errorf("centrifugo connection lost: code=%d reason=%s", event.Code, event.Reason))
			}
			if cfg.onReconnecting != nil {
				cfg.onReconnecting()
			}
		}
	})
	client.OnDisconnected(func(event centrifuge.DisconnectedEvent) {
		adapter.connected.Store(false)
		if cfg.onConnectionLost != nil {
			cfg.onConnectionLost(fmt.Errorf("centrifugo disconnected: code=%d reason=%s", event.Code, event.Reason))
		}
	})

	adapter.client = client
	return adapter, nil
}

func (impl *AdapterImpl) SignalingID() string {
	return "centrifugo:" + impl.cfg.wsURL + ":" + impl.cfg.baseChannel
}

func (impl *AdapterImpl) Publish(roomID string, typ signaling.MessageType, payload []byte) error {
	if err := signaling.ValidateRoomID(roomID); err != nil {
		return err
	}
	if err := signaling.ValidateMessageType(typ); err != nil {
		return err
	}
	return impl.publish(impl.topic(roomID, typ), payload)
}

func (impl *AdapterImpl) Subscribe(roomID string, typ signaling.MessageType, handler signaling.Handler) (signaling.Subscription, error) {
	if err := signaling.ValidateRoomID(roomID); err != nil {
		return nil, err
	}
	if err := signaling.ValidateMessageType(typ); err != nil {
		return nil, err
	}
	id, err := impl.on(impl.topic(roomID, typ), handler)
	if err != nil {
		return nil, err
	}
	return signaling.NewSubscription(func() error { return impl.unsubscribe(*id) }), nil
}

func WithAPIToken(apiURL, apiKey string) option {
	return func(cfg *config) error {
		cfg.apiURL = apiURL
		cfg.apiKey = apiKey
		return nil
	}
}

func WithConnectToken(token string) option {
	return func(cfg *config) error {
		cfg.token = token
		return nil
	}
}

// WithReconnectBackoff configures the minimum and maximum delay between
// Centrifugo reconnect attempts.
func WithReconnectBackoff(minDelay, maxDelay time.Duration) option {
	return func(cfg *config) error {
		if minDelay <= 0 || maxDelay <= 0 || maxDelay < minDelay {
			return errors.New("Centrifugo reconnect backoff must be positive and max >= min")
		}
		cfg.reconnectMin = minDelay
		cfg.reconnectMax = maxDelay
		return nil
	}
}

func WithOnConnect(handler func()) option {
	return func(cfg *config) error {
		cfg.onConnect = handler
		return nil
	}
}

func WithConnectionLostHandler(handler func(error)) option {
	return func(cfg *config) error {
		cfg.onConnectionLost = handler
		return nil
	}
}

func WithReconnectingHandler(handler func()) option {
	return func(cfg *config) error {
		cfg.onReconnecting = handler
		return nil
	}
}

// WithBaseChannel changes the prefix used before roomID and message type.
// For example, "sensors:telekit" maps to
// sensors:telekit:<room>:<type>. Both peers must use the same value.
func WithBaseChannel(baseChannel string) option {
	return func(cfg *config) error {
		if err := signaling.ValidateRoutePrefix(baseChannel, ':'); err != nil {
			return err
		}
		cfg.baseChannel = baseChannel
		return nil
	}
}
