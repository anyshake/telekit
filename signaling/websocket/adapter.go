package websocket

import (
	"encoding/json"
	"errors"
	"time"

	"github.com/anyshake/telekit/signaling"
	gorilla "github.com/gorilla/websocket"
)

func (a *Adapter) SignalingID() string { return "websocket:" + a.baseURL }

// Connect marks the adapter ready. Physical WebSockets are opened lazily
// because the room ID is part of their path.
func (a *Adapter) Connect() error {
	a.mu.Lock()
	a.connected = true
	a.mu.Unlock()
	return nil
}

func (a *Adapter) Disconnect() error {
	a.mu.Lock()
	rooms := a.rooms
	a.rooms = make(map[string]*roomConn)
	a.connected = false
	a.mu.Unlock()
	for _, room := range rooms {
		room.close()
	}
	return nil
}

func (a *Adapter) Publish(roomID string, typ signaling.MessageType, payload []byte) error {
	if err := signaling.ValidateMessageType(typ); err != nil {
		return err
	}
	room, err := a.ensureRoom(roomID)
	if err != nil {
		return err
	}
	data, err := json.Marshal(packet{Type: typ, Payload: payload})
	if err != nil {
		return err
	}
	room.writeMu.Lock()
	defer room.writeMu.Unlock()
	if room.conn == nil {
		return signaling.ErrClosed
	}
	_ = room.conn.SetWriteDeadline(time.Now().Add(websocketWriteTimeout))
	err = room.conn.WriteMessage(gorilla.TextMessage, data)
	_ = room.conn.SetWriteDeadline(time.Time{})
	if err != nil {
		_ = room.conn.Close()
		return err
	}
	return nil
}

func (a *Adapter) Subscribe(roomID string, typ signaling.MessageType, handler signaling.Handler) (signaling.Subscription, error) {
	if handler == nil {
		return nil, errors.New("handler is nil")
	}
	if err := signaling.ValidateMessageType(typ); err != nil {
		return nil, err
	}
	room, err := a.ensureRoom(roomID)
	if err != nil {
		return nil, err
	}
	id := a.nextID.Add(1)
	room.handlerM.Lock()
	if room.handlers[typ] == nil {
		room.handlers[typ] = make(map[uint64]signaling.Handler)
	}
	room.handlers[typ][id] = handler
	room.handlerM.Unlock()
	return signaling.NewSubscription(func() error {
		room.handlerM.Lock()
		delete(room.handlers[typ], id)
		room.handlerM.Unlock()
		return nil
	}), nil
}
