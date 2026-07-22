package mqtt

import (
	"errors"
	"fmt"
	"time"

	"github.com/anyshake/telekit/signaling"
	mqtt "github.com/eclipse/paho.mqtt.golang"
)

const resubscribeRetryMaximum = 4 * time.Second

func (impl *MqttAdapterImpl) topic(roomID string, typ signaling.MessageType) string {
	return fmt.Sprintf("%s/%s/%s", impl.config.baseTopic, roomID, typ)
}

func (impl *MqttAdapterImpl) handleConnectionLost(err error) {
	impl.generation.Add(1) // Cancels any older resubscribe retry loop.
	impl.markSubscriptionsStale()
	if impl.config.onConnectionLost != nil {
		impl.config.onConnectionLost(err)
	}
}

func (impl *MqttAdapterImpl) handleConnected(client mqtt.Client) {
	generation := impl.generation.Add(1)
	impl.markSubscriptionsStale()

	backoff := 250 * time.Millisecond
	for {
		if impl.generation.Load() != generation || !client.IsConnectionOpen() {
			return
		}

		topics := impl.subscriptionTopics()
		allSubscribed := true
		for _, topic := range topics {
			if err := impl.ensureSubscribed(client, topic); err != nil {
				allSubscribed = false
				if impl.config.onSubscriptionError != nil {
					impl.config.onSubscriptionError(topic, err)
				} else {
					mqtt.ERROR.Printf("[telekit] failed to restore subscription %q: %v", topic, err)
				}
			}
		}
		if allSubscribed && impl.generation.Load() == generation && client.IsConnectionOpen() && impl.subscriptionsReady() {
			if impl.config.onConnect != nil {
				impl.config.onConnect()
			}
			return
		}

		time.Sleep(backoff)
		backoff = min(backoff*2, resubscribeRetryMaximum)
	}
}

func (impl *MqttAdapterImpl) subscriptionsReady() bool {
	impl.mu.Lock()
	defer impl.mu.Unlock()
	for _, group := range impl.subs {
		if len(group.handlers) > 0 && !group.subscribed {
			return false
		}
	}
	return true
}

func (impl *MqttAdapterImpl) markSubscriptionsStale() {
	impl.mu.Lock()
	defer impl.mu.Unlock()
	for _, group := range impl.subs {
		group.subscribed = false
	}
}

func (impl *MqttAdapterImpl) subscriptionTopics() []string {
	impl.mu.Lock()
	defer impl.mu.Unlock()
	topics := make([]string, 0, len(impl.subs))
	for topic, group := range impl.subs {
		if len(group.handlers) > 0 {
			topics = append(topics, topic)
		}
	}
	return topics
}

// enqueueDispatch returns immediately to Paho's ordered message router, then
// runs application handlers on a separate ordered queue. This keeps slow or
// re-entrant handlers from starving MQTT acknowledgements and keep-alive.
func (impl *MqttAdapterImpl) enqueueDispatch(topic string, payload []byte) {
	impl.mu.Lock()
	maxMessages := impl.config.dispatchQueueMessages
	if maxMessages <= 0 {
		maxMessages = defaultDispatchQueueMessages
	}
	maxBytes := impl.config.dispatchQueueBytes
	if maxBytes <= 0 {
		maxBytes = defaultDispatchQueueBytes
	}
	if len(impl.dispatchQueue) >= maxMessages || len(payload) > maxBytes-impl.dispatchBytes {
		handler := impl.config.onDispatchOverflow
		impl.mu.Unlock()
		if handler != nil {
			handler(topic)
		}
		return
	}
	impl.dispatchQueue = append(impl.dispatchQueue, queuedMessage{
		topic:   topic,
		payload: append([]byte(nil), payload...),
	})
	impl.dispatchBytes += len(payload)
	if impl.dispatching {
		impl.mu.Unlock()
		return
	}
	impl.dispatching = true
	impl.mu.Unlock()
	go impl.drainDispatchQueue()
}

func (impl *MqttAdapterImpl) drainDispatchQueue() {
	for {
		impl.mu.Lock()
		if len(impl.dispatchQueue) == 0 {
			impl.dispatching = false
			impl.mu.Unlock()
			return
		}
		message := impl.dispatchQueue[0]
		impl.dispatchQueue[0] = queuedMessage{}
		impl.dispatchQueue = impl.dispatchQueue[1:]
		impl.dispatchBytes -= len(message.payload)

		group := impl.subs[message.topic]
		handlers := make([]func([]byte), 0)
		if group != nil {
			handlers = make([]func([]byte), 0, len(group.handlers))
			for _, sub := range group.handlers {
				handlers = append(handlers, sub.handler)
			}
		}
		impl.mu.Unlock()

		for _, handler := range handlers {
			handler(append([]byte(nil), message.payload...))
		}
	}
}

// ensureSubscribed serializes native SUBSCRIBE operations for a topic. It is
// used both by application subscriptions and by automatic reconnect recovery.
func (impl *MqttAdapterImpl) ensureSubscribed(client mqtt.Client, topic string) error {
	for {
		impl.mu.Lock()
		group := impl.subs[topic]
		if group == nil || len(group.handlers) == 0 {
			impl.mu.Unlock()
			return nil
		}
		if group.subscribed {
			impl.mu.Unlock()
			return nil
		}
		if group.subscribing {
			ready := group.ready
			impl.mu.Unlock()
			<-ready
			continue
		}
		group.subscribing = true
		group.ready = make(chan struct{})
		ready := group.ready
		generation := impl.generation.Load()
		impl.mu.Unlock()

		token := client.Subscribe(topic, impl.config.qos, func(_ mqtt.Client, msg mqtt.Message) {
			impl.enqueueDispatch(topic, msg.Payload())
		})
		err := impl.waitToken(token, "subscribe", topic)

		impl.mu.Lock()
		// The group may have been removed while SUBSCRIBE was in flight.
		group.subscribed = err == nil && impl.generation.Load() == generation
		group.subscribing = false
		close(ready)
		impl.mu.Unlock()
		return err
	}
}

func (impl *MqttAdapterImpl) unsubscribe(id int) error {
	var topic string
	var group *topicSubscriptions
	impl.mu.Lock()
	for candidate, candidateGroup := range impl.subs {
		if _, ok := candidateGroup.handlers[id]; !ok {
			continue
		}
		delete(candidateGroup.handlers, id)
		if len(candidateGroup.handlers) == 0 {
			topic = candidate
			group = candidateGroup
		}
		break
	}
	impl.mu.Unlock()

	if topic == "" {
		return nil
	}
	for {
		impl.mu.Lock()
		if impl.subs[topic] != group || len(group.handlers) > 0 {
			impl.mu.Unlock()
			return nil
		}
		if group.subscribing {
			ready := group.ready
			impl.mu.Unlock()
			<-ready
			continue
		}
		if !group.subscribed || !impl.client.IsConnectionOpen() {
			delete(impl.subs, topic)
			impl.mu.Unlock()
			return nil
		}

		// Keep the empty group visible until UNSUBSCRIBE finishes. A concurrent
		// On call will join it and wait, then issue a fresh SUBSCRIBE instead of
		// having its subscription accidentally removed by this operation.
		group.subscribing = true
		group.ready = make(chan struct{})
		ready := group.ready
		impl.mu.Unlock()

		err := impl.waitToken(impl.client.Unsubscribe(topic), "unsubscribe", topic)

		impl.mu.Lock()
		group.subscribed = false
		group.subscribing = false
		close(ready)
		if len(group.handlers) == 0 && impl.subs[topic] == group {
			delete(impl.subs, topic)
		}
		impl.mu.Unlock()
		return err
	}
}

func (impl *MqttAdapterImpl) publish(topic string, msg []byte) error {
	if impl.client == nil {
		return errors.New("mqtt client is nil")
	}
	if !impl.client.IsConnectionOpen() {
		return signaling.ErrClosed
	}
	return impl.waitToken(impl.client.Publish(topic, impl.config.qos, false, msg), "publish", topic)
}

func (impl *MqttAdapterImpl) on(topic string, handler func([]byte)) (*int, error) {
	if impl.client == nil {
		return nil, errors.New("mqtt client is nil")
	}
	if handler == nil {
		return nil, errors.New("handler is nil")
	}
	if !impl.client.IsConnectionOpen() {
		return nil, signaling.ErrClosed
	}

	id := int(impl.nextID.Add(1))
	impl.mu.Lock()
	group := impl.subs[topic]
	if group == nil {
		group = &topicSubscriptions{handlers: make(map[int]*subscription)}
		impl.subs[topic] = group
	}
	group.handlers[id] = &subscription{topic: topic, handler: handler}
	impl.mu.Unlock()

	if err := impl.ensureSubscribed(impl.client, topic); err != nil {
		_ = impl.unsubscribe(id)
		return nil, err
	}
	return &id, nil
}

func (impl *MqttAdapterImpl) waitToken(token mqtt.Token, operation, topic string) error {
	if !token.WaitTimeout(impl.config.operationTimeout) {
		return fmt.Errorf("MQTT %s timed out for topic %q after %s", operation, topic, impl.config.operationTimeout)
	}
	if err := token.Error(); err != nil {
		return fmt.Errorf("MQTT %s failed for topic %q: %w", operation, topic, err)
	}
	return nil
}
