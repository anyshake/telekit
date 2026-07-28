package websocket

import (
	"encoding/json"
	"time"

	"github.com/anyshake/telekit/signaling"
	"github.com/anyshake/telekit/signaling/websocket/broker"
	gorilla "github.com/gorilla/websocket"
)

func (a *Adapter) readLoop(roomID string, room *roomConn, conn *gorilla.Conn) {
	for {
		_, data, err := conn.ReadMessage()
		if err != nil {
			if a.onConnectionLost != nil {
				a.onConnectionLost(err)
			}
			if a.onReconnecting != nil {
				a.onReconnecting()
			}
			_ = conn.Close()
			room.writeMu.Lock()
			if room.conn == conn {
				room.conn = nil
			}
			room.writeMu.Unlock()

			newConn, ok := a.reconnectRoom(roomID, room)
			if !ok {
				a.removeRoom(roomID, room)
				return
			}
			conn = newConn
			if a.onConnect != nil {
				a.onConnect()
			}
			continue
		}
		var p packet
		if json.Unmarshal(data, &p) != nil {
			continue
		}
		room.handlerM.RLock()
		handlers := make([]signaling.Handler, 0, len(room.handlers[p.Type]))
		for _, handler := range room.handlers[p.Type] {
			handlers = append(handlers, handler)
		}
		room.handlerM.RUnlock()
		for _, handler := range handlers {
			handler(append([]byte(nil), p.Payload...))
		}
	}
}

func (a *Adapter) reconnectRoom(roomID string, room *roomConn) (*gorilla.Conn, bool) {
	delay := a.reconnectMin
	maxDelay := a.reconnectMax
	if delay <= 0 {
		delay = websocketReconnectMin
	}
	if maxDelay < delay {
		maxDelay = websocketReconnectMax
	}
	for {
		if !a.roomActive(roomID, room) {
			return nil, false
		}

		endpoint, err := a.roomURL(roomID)
		if err == nil {
			conn, response, dialErr := a.dialer.Dial(endpoint, a.headers.Clone())
			if dialErr == nil {
				conn.SetReadLimit(broker.MaxMessageSize)
				if !a.roomActive(roomID, room) {
					_ = conn.Close()
					return nil, false
				}
				room.writeMu.Lock()
				room.conn = conn
				room.writeMu.Unlock()
				return conn, true
			}
			closeHandshakeResponse(response)
		}

		timer := time.NewTimer(delay)
		select {
		case <-timer.C:
		case <-room.done:
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			return nil, false
		}
		if delay < maxDelay {
			delay *= 2
			if delay > maxDelay {
				delay = maxDelay
			}
		}
	}
}
