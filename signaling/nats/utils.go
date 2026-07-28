package nats

import (
	"fmt"

	"github.com/anyshake/telekit/signaling"
	natsgo "github.com/nats-io/nats.go"
)

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
