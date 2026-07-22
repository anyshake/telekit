package mqtt

import (
	"errors"
	"time"

	"github.com/anyshake/telekit/signaling"
	mqtt "github.com/eclipse/paho.mqtt.golang"
)

const (
	DefaultBaseTopic                  = "telekit"
	defaultQoS                   byte = 1
	defaultOperationTimeout           = 15 * time.Second
	defaultReconnectMaximum           = 30 * time.Second
	defaultDispatchQueueMessages      = 1024
	defaultDispatchQueueBytes         = 16 << 20
)

func NewMQTTAdapter(endpoint string, opts ...option) (signaling.IAdapter, error) {
	cfg := &config{
		brokerOpts:            mqtt.NewClientOptions(),
		baseTopic:             DefaultBaseTopic,
		qos:                   defaultQoS,
		operationTimeout:      defaultOperationTimeout,
		dispatchQueueMessages: defaultDispatchQueueMessages,
		dispatchQueueBytes:    defaultDispatchQueueBytes,
	}
	cfg.brokerOpts.AddBroker(endpoint)
	cfg.brokerOpts.SetAutoReconnect(true)
	cfg.brokerOpts.SetResumeSubs(true)
	cfg.brokerOpts.SetMaxReconnectInterval(defaultReconnectMaximum)
	cfg.brokerOpts.SetWriteTimeout(defaultOperationTimeout)

	for _, opt := range opts {
		if err := opt(cfg); err != nil {
			return nil, err
		}
	}

	impl := &MqttAdapterImpl{
		endpoint: endpoint,
		config:   cfg,
		subs:     make(map[string]*topicSubscriptions),
	}
	cfg.brokerOpts.SetConnectionLostHandler(func(client mqtt.Client, err error) {
		impl.handleConnectionLost(err)
		if cfg.onConnectionLost == nil {
			mqtt.DefaultConnectionLostHandler(client, err)
		}
	})
	cfg.brokerOpts.SetReconnectingHandler(func(_ mqtt.Client, _ *mqtt.ClientOptions) {
		if cfg.onReconnecting != nil {
			cfg.onReconnecting()
		}
	})
	cfg.brokerOpts.SetOnConnectHandler(func(client mqtt.Client) {
		impl.handleConnected(client)
	})
	impl.client = mqtt.NewClient(cfg.brokerOpts)
	return impl, nil
}

func (impl *MqttAdapterImpl) SignalingID() string {
	return "mqtt:" + impl.endpoint + ":" + impl.config.baseTopic
}

func (impl *MqttAdapterImpl) Publish(roomID string, typ signaling.MessageType, payload []byte) error {
	if err := signaling.ValidateRoomID(roomID); err != nil {
		return err
	}
	if err := signaling.ValidateMessageType(typ); err != nil {
		return err
	}
	return impl.publish(impl.topic(roomID, typ), payload)
}

func (impl *MqttAdapterImpl) Subscribe(roomID string, typ signaling.MessageType, handler signaling.Handler) (signaling.Subscription, error) {
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

func WithClientID(clientID string) option {
	return func(cfg *config) error {
		cfg.brokerOpts.SetClientID(clientID)
		return nil
	}
}

// WithBaseTopic changes the prefix used before roomID and message type.
// For example, "sensors/telekit" maps to
// sensors/telekit/<room>/<type>. Both peers must use the same value.
func WithBaseTopic(baseTopic string) option {
	return func(cfg *config) error {
		if err := signaling.ValidateRoutePrefix(baseTopic, '/'); err != nil {
			return err
		}
		cfg.baseTopic = baseTopic
		return nil
	}
}

func WithCredentials(username, password string) option {
	return func(cfg *config) error {
		cfg.brokerOpts.SetUsername(username)
		cfg.brokerOpts.SetPassword(password)
		return nil
	}
}

func WithConnectTimeout(timeout time.Duration) option {
	return func(cfg *config) error {
		if timeout <= 0 {
			return errors.New("connect timeout must be positive")
		}
		cfg.brokerOpts.SetConnectTimeout(timeout)
		return nil
	}
}

func WithKeepAlive(keepAlive time.Duration) option {
	return func(cfg *config) error {
		if keepAlive <= 0 {
			return errors.New("keep alive must be positive")
		}
		cfg.brokerOpts.SetKeepAlive(keepAlive)
		return nil
	}
}

func WithPingTimeout(timeout time.Duration) option {
	return func(cfg *config) error {
		if timeout <= 0 {
			return errors.New("ping timeout must be positive")
		}
		cfg.brokerOpts.SetPingTimeout(timeout)
		return nil
	}
}

func WithMaxReconnectInterval(interval time.Duration) option {
	return func(cfg *config) error {
		if interval <= 0 {
			return errors.New("maximum reconnect interval must be positive")
		}
		cfg.brokerOpts.SetMaxReconnectInterval(interval)
		return nil
	}
}

// WithQoS sets the QoS used for signaling publish and subscribe operations.
// QoS 1 is the default so transient network loss does not silently discard a
// signaling packet after it has reached the broker.
func WithQoS(qos byte) option {
	return func(cfg *config) error {
		if qos > 2 {
			return errors.New("MQTT QoS must be 0, 1, or 2")
		}
		cfg.qos = qos
		return nil
	}
}

func WithOperationTimeout(timeout time.Duration) option {
	return func(cfg *config) error {
		if timeout <= 0 {
			return errors.New("MQTT operation timeout must be positive")
		}
		cfg.operationTimeout = timeout
		cfg.brokerOpts.SetWriteTimeout(timeout)
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

func WithSubscriptionErrorHandler(handler func(topic string, err error)) option {
	return func(cfg *config) error {
		cfg.onSubscriptionError = handler
		return nil
	}
}

// WithDispatchQueueLimits bounds the queue between Paho's receive callback
// and application signaling handlers. New messages are dropped on overflow so
// MQTT acknowledgements and keep-alives cannot be starved by an unbounded
// allocation backlog.
func WithDispatchQueueLimits(maxMessages, maxBytes int) option {
	return func(cfg *config) error {
		if maxMessages < 1 || maxBytes < 1 {
			return errors.New("MQTT dispatch queue limits must be positive")
		}
		cfg.dispatchQueueMessages = maxMessages
		cfg.dispatchQueueBytes = maxBytes
		return nil
	}
}

func WithDispatchOverflowHandler(handler func(topic string)) option {
	return func(cfg *config) error {
		cfg.onDispatchOverflow = handler
		return nil
	}
}
