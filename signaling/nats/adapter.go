package nats

import (
	"errors"
	"fmt"

	"github.com/anyshake/telekit/signaling"
	natsgo "github.com/nats-io/nats.go"
)

func (a *Adapter) SignalingID() string {
	return fmt.Sprintf("nats:%s:%s", a.url, a.baseSubject)
}

func (a *Adapter) Publish(roomID string, typ signaling.MessageType, payload []byte) error {
	nc, subject, err := a.connectionAndSubject(roomID, typ)
	if err != nil {
		return err
	}
	return nc.Publish(subject, payload)
}

func (a *Adapter) Subscribe(roomID string, typ signaling.MessageType, handler signaling.Handler) (signaling.Subscription, error) {
	if handler == nil {
		return nil, errors.New("handler is nil")
	}
	nc, subject, err := a.connectionAndSubject(roomID, typ)
	if err != nil {
		return nil, err
	}
	sub, err := nc.Subscribe(subject, func(msg *natsgo.Msg) {
		handler(append([]byte(nil), msg.Data...))
	})
	if err != nil {
		return nil, err
	}
	if err := nc.Flush(); err != nil {
		_ = sub.Unsubscribe()
		return nil, err
	}
	return signaling.NewSubscription(sub.Unsubscribe), nil
}
