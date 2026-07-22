package mqtt

import (
	"sync"
	"sync/atomic"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
)

type option func(*config) error

type config struct {
	brokerOpts            *mqtt.ClientOptions
	baseTopic             string
	qos                   byte
	operationTimeout      time.Duration
	onConnect             func()
	onConnectionLost      func(error)
	onReconnecting        func()
	onSubscriptionError   func(topic string, err error)
	dispatchQueueMessages int
	dispatchQueueBytes    int
	onDispatchOverflow    func(topic string)
}

type subscription struct {
	topic   string
	handler func([]byte)
}

type topicSubscriptions struct {
	handlers    map[int]*subscription
	subscribed  bool
	subscribing bool
	ready       chan struct{}
}

type queuedMessage struct {
	topic   string
	payload []byte
}

type MqttAdapterImpl struct {
	endpoint string
	config   *config
	client   mqtt.Client

	subs       map[string]*topicSubscriptions
	mu         sync.Mutex
	nextID     atomic.Int64
	generation atomic.Uint64

	dispatchQueue []queuedMessage
	dispatchBytes int
	dispatching   bool
}
