package mqtt

import (
	"bytes"
	"sync"
	"testing"
	"time"

	"github.com/anyshake/telekit/signaling"
	paho "github.com/eclipse/paho.mqtt.golang"
)

func TestReconnectRestoresNativeSubscriptions(t *testing.T) {
	client := newFakeClient()
	connected := 0
	impl := &MqttAdapterImpl{
		config: &config{
			qos:              1,
			operationTimeout: time.Second,
			onConnect:        func() { connected++ },
		},
		client: client,
		subs:   make(map[string]*topicSubscriptions),
	}

	received := make(chan []byte, 2)
	sub, err := impl.Subscribe("room", signaling.MessageHello, func(payload []byte) {
		received <- payload
	})
	if err != nil {
		t.Fatal(err)
	}
	topic := impl.topic("room", signaling.MessageHello)
	if got := client.subscribeCount(topic); got != 1 {
		t.Fatalf("initial native subscriptions = %d, want 1", got)
	}
	if qos := client.subscriptionQoS(topic); qos != 1 {
		t.Fatalf("subscription QoS = %d, want 1", qos)
	}
	client.deliver(topic, []byte("before"))
	if got := <-received; !bytes.Equal(got, []byte("before")) {
		t.Fatalf("received %q", got)
	}

	client.setOpen(false)
	impl.handleConnectionLost(nil)
	client.setOpen(true)
	impl.handleConnected(client)
	if got := client.subscribeCount(topic); got != 2 {
		t.Fatalf("native subscriptions after reconnect = %d, want 2", got)
	}
	if connected != 1 {
		t.Fatalf("onConnect calls = %d, want 1", connected)
	}
	client.deliver(topic, []byte("after"))
	if got := <-received; !bytes.Equal(got, []byte("after")) {
		t.Fatalf("received %q", got)
	}

	if err := sub.Unsubscribe(); err != nil {
		t.Fatal(err)
	}
	if got := client.unsubscribeCount(topic); got != 1 {
		t.Fatalf("native unsubscriptions = %d, want 1", got)
	}
	impl.handleConnectionLost(nil)
	impl.handleConnected(client)
	if got := client.subscribeCount(topic); got != 2 {
		t.Fatalf("removed topic was restored; subscriptions = %d", got)
	}
}

func TestPublishUsesConfiguredQoS(t *testing.T) {
	client := newFakeClient()
	impl := &MqttAdapterImpl{
		config: &config{qos: 1, operationTimeout: time.Second},
		client: client,
		subs:   make(map[string]*topicSubscriptions),
	}
	if err := impl.Publish("room", signaling.MessageOffer, []byte("signal")); err != nil {
		t.Fatal(err)
	}
	if client.lastPublishQoS() != 1 {
		t.Fatalf("publish QoS = %d, want 1", client.lastPublishQoS())
	}
}

func TestDispatchPreservesMessageOrder(t *testing.T) {
	client := newFakeClient()
	impl := &MqttAdapterImpl{
		config: &config{qos: 1, operationTimeout: time.Second},
		client: client,
		subs:   make(map[string]*topicSubscriptions),
	}
	received := make(chan byte, 64)
	if _, err := impl.Subscribe("room", signaling.MessageAnswer, func(payload []byte) {
		received <- payload[0]
	}); err != nil {
		t.Fatal(err)
	}
	topic := impl.topic("room", signaling.MessageAnswer)
	for i := byte(0); i < 64; i++ {
		client.deliver(topic, []byte{i})
	}
	for i := byte(0); i < 64; i++ {
		if got := <-received; got != i {
			t.Fatalf("message %d arrived as %d", i, got)
		}
	}
}

func TestDefaultReconnectOptions(t *testing.T) {
	adapter, err := NewMQTTAdapter("tcp://127.0.0.1:1883")
	if err != nil {
		t.Fatal(err)
	}
	impl := adapter.(*MqttAdapterImpl)
	options := impl.client.OptionsReader()

	if !options.AutoReconnect() {
		t.Fatal("automatic reconnect is disabled")
	}
	if !options.ResumeSubs() {
		t.Fatal("pending subscription recovery is disabled")
	}
	if !options.Order() {
		t.Fatal("Paho message ordering is disabled")
	}
	if options.MaxReconnectInterval() != defaultReconnectMaximum {
		t.Fatalf("maximum reconnect interval = %s, want %s", options.MaxReconnectInterval(), defaultReconnectMaximum)
	}
	if impl.config.qos != defaultQoS {
		t.Fatalf("default QoS = %d, want %d", impl.config.qos, defaultQoS)
	}
}

type fakeToken struct {
	done chan struct{}
	err  error
}

func completedToken(err error) *fakeToken {
	done := make(chan struct{})
	close(done)
	return &fakeToken{done: done, err: err}
}

func (t *fakeToken) Wait() bool                     { <-t.done; return true }
func (t *fakeToken) WaitTimeout(time.Duration) bool { <-t.done; return true }
func (t *fakeToken) Done() <-chan struct{}          { return t.done }
func (t *fakeToken) Error() error                   { return t.err }

type fakeClient struct {
	mu               sync.Mutex
	open             bool
	subscribeCalls   map[string]int
	unsubscribeCalls map[string]int
	qos              map[string]byte
	handlers         map[string]paho.MessageHandler
	publishQoS       byte
}

func newFakeClient() *fakeClient {
	return &fakeClient{
		open:             true,
		subscribeCalls:   make(map[string]int),
		unsubscribeCalls: make(map[string]int),
		qos:              make(map[string]byte),
		handlers:         make(map[string]paho.MessageHandler),
	}
}

func (c *fakeClient) IsConnected() bool      { return c.IsConnectionOpen() }
func (c *fakeClient) IsConnectionOpen() bool { c.mu.Lock(); defer c.mu.Unlock(); return c.open }
func (c *fakeClient) Connect() paho.Token    { c.setOpen(true); return completedToken(nil) }
func (c *fakeClient) Disconnect(uint)        { c.setOpen(false) }
func (c *fakeClient) Publish(_ string, qos byte, _ bool, _ any) paho.Token {
	c.mu.Lock()
	c.publishQoS = qos
	c.mu.Unlock()
	return completedToken(nil)
}
func (c *fakeClient) Subscribe(topic string, qos byte, handler paho.MessageHandler) paho.Token {
	c.mu.Lock()
	c.subscribeCalls[topic]++
	c.qos[topic] = qos
	c.handlers[topic] = handler
	c.mu.Unlock()
	return completedToken(nil)
}
func (c *fakeClient) SubscribeMultiple(filters map[string]byte, handler paho.MessageHandler) paho.Token {
	for topic, qos := range filters {
		c.Subscribe(topic, qos, handler)
	}
	return completedToken(nil)
}
func (c *fakeClient) Unsubscribe(topics ...string) paho.Token {
	c.mu.Lock()
	for _, topic := range topics {
		c.unsubscribeCalls[topic]++
		delete(c.handlers, topic)
	}
	c.mu.Unlock()
	return completedToken(nil)
}
func (c *fakeClient) AddRoute(topic string, handler paho.MessageHandler) {
	c.mu.Lock()
	c.handlers[topic] = handler
	c.mu.Unlock()
}
func (c *fakeClient) OptionsReader() paho.ClientOptionsReader { return paho.ClientOptionsReader{} }

func (c *fakeClient) setOpen(open bool) {
	c.mu.Lock()
	c.open = open
	c.mu.Unlock()
}

func (c *fakeClient) subscribeCount(topic string) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.subscribeCalls[topic]
}

func (c *fakeClient) unsubscribeCount(topic string) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.unsubscribeCalls[topic]
}

func (c *fakeClient) subscriptionQoS(topic string) byte {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.qos[topic]
}

func (c *fakeClient) lastPublishQoS() byte {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.publishQoS
}

func (c *fakeClient) deliver(topic string, payload []byte) {
	c.mu.Lock()
	handler := c.handlers[topic]
	c.mu.Unlock()
	handler(c, fakeMessage{topic: topic, payload: payload})
}

type fakeMessage struct {
	topic   string
	payload []byte
}

func (fakeMessage) Duplicate() bool   { return false }
func (fakeMessage) Qos() byte         { return 1 }
func (fakeMessage) Retained() bool    { return false }
func (m fakeMessage) Topic() string   { return m.topic }
func (fakeMessage) MessageID() uint16 { return 1 }
func (m fakeMessage) Payload() []byte { return m.payload }
func (fakeMessage) Ack()              {}
