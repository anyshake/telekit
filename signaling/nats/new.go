// Package nats implements signaling.Adapter with NATS subjects.
package nats

import (
	"errors"
	"fmt"
	"sync"

	"github.com/anyshake/telekit/signaling"
	natsgo "github.com/nats-io/nats.go"
)

type Adapter struct {
	url         string
	baseSubject string
	opts        []natsgo.Option

	mu sync.RWMutex
	nc *natsgo.Conn
}

const DefaultBaseSubject = "telekit"

func (a *Adapter) SignalingID() string {
	return "nats:" + a.url + ":" + a.baseSubject
}

func NewAdapter(url string, opts ...natsgo.Option) (signaling.Adapter, error) {
	return NewAdapterWithBaseSubject(url, DefaultBaseSubject, opts...)
}

// NewAdapterWithBaseSubject creates an adapter whose subjects are rooted at
// baseSubject. For example, "sensors.telekit" maps to
// sensors.telekit.<room>.<type>. Both peers must use the same value.
func NewAdapterWithBaseSubject(url, baseSubject string, opts ...natsgo.Option) (signaling.Adapter, error) {
	if url == "" {
		return nil, errors.New("NATS URL is required")
	}
	if err := signaling.ValidateRoutePrefix(baseSubject, '.'); err != nil {
		return nil, err
	}
	return &Adapter{url: url, baseSubject: baseSubject, opts: opts}, nil
}

func (a *Adapter) Connect() error {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.nc != nil && !a.nc.IsClosed() {
		return nil
	}
	nc, err := natsgo.Connect(a.url, a.opts...)
	if err != nil {
		return err
	}
	a.nc = nc
	return nil
}

func (a *Adapter) Disconnect() error {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.nc == nil {
		return nil
	}
	a.nc.Close()
	a.nc = nil
	return nil
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

func (a *Adapter) connectionAndSubject(roomID string, typ signaling.MessageType) (*natsgo.Conn, string, error) {
	if err := signaling.ValidateRoomID(roomID); err != nil {
		return nil, "", err
	}
	if err := signaling.ValidateMessageType(typ); err != nil {
		return nil, "", err
	}
	a.mu.RLock()
	nc := a.nc
	a.mu.RUnlock()
	if nc == nil || nc.IsClosed() {
		return nil, "", signaling.ErrClosed
	}
	return nc, a.subject(roomID, typ), nil
}

func (a *Adapter) subject(roomID string, typ signaling.MessageType) string {
	return fmt.Sprintf("%s.%s.%s", a.baseSubject, roomID, typ)
}
