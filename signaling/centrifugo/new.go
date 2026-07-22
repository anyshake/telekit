package centrifugo

import (
	"errors"

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

	client := centrifuge.NewJsonClient(
		cfg.wsURL,
		centrifuge.Config{
			Token: cfg.token,
		},
	)

	return &AdapterImpl{
		cfg:    cfg,
		client: client,
		subs:   make(map[string]*haxmap.Map[int, *subscription]),
	}, nil
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
