package centrifugo

import (
	"context"
	"errors"
	"fmt"

	"github.com/alphadose/haxmap"
	"github.com/anyshake/telekit/signaling"
	"github.com/centrifugal/centrifuge-go"
)

func (impl *AdapterImpl) topic(roomID string, typ signaling.MessageType) string {
	return fmt.Sprintf("%s:%s:%s", impl.cfg.baseChannel, roomID, typ)
}

func (impl *AdapterImpl) subscribeOnce(topic string) error {
	sub, err := impl.client.NewSubscription(topic)
	if err != nil {
		return err
	}

	sub.OnPublication(func(e centrifuge.PublicationEvent) {
		impl.mu.Lock()
		handlers := impl.subs[topic]
		impl.mu.Unlock()

		if handlers == nil {
			return
		}

		for _, h := range handlers.Iterator() {
			h.handler(e.Data)
		}
	})

	return sub.Subscribe()
}

func (impl *AdapterImpl) publish(topic string, msg []byte) error {
	_, err := impl.client.Publish(
		context.Background(),
		topic,
		msg,
	)
	return err
}

func (impl *AdapterImpl) on(topic string, handler func([]byte)) (*int, error) {
	if handler == nil {
		return nil, errors.New("handler is nil")
	}

	impl.mu.Lock()
	defer impl.mu.Unlock()

	if impl.subs == nil {
		impl.subs = make(map[string]*haxmap.Map[int, *subscription])
	}

	handlers, ok := impl.subs[topic]
	if !ok {
		handlers = haxmap.New[int, *subscription]()
		impl.subs[topic] = handlers

		if err := impl.subscribeOnce(topic); err != nil {
			return nil, err
		}
	}

	id := int(impl.nextID.Add(1))
	handlers.Set(id, &subscription{handler: handler})

	return &id, nil
}

func (impl *AdapterImpl) unsubscribe(id int) error {
	impl.mu.Lock()
	defer impl.mu.Unlock()

	for _, handlers := range impl.subs {
		handlers.Del(id)
	}
	return nil
}
