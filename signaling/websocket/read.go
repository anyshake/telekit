package websocket

import (
	"encoding/json"
	"time"

	"github.com/anyshake/telekit/signaling"
	gorilla "github.com/gorilla/websocket"
)

func (a *Adapter) readLoop(roomID string, room *roomConn, conn *gorilla.Conn) {
	for {
		pingDone := make(chan struct{})
		go pingLoop(room, conn, pingDone)
		var readErr error
		for {
			_, data, err := conn.ReadMessage()
			if err != nil {
				readErr = err
				close(pingDone)
				break
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

		if !a.roomActive(roomID, room) {
			return
		}
		if a.onConnectionLost != nil {
			a.onConnectionLost(readErr)
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
	}
}

func pingLoop(room *roomConn, conn *gorilla.Conn, done <-chan struct{}) {
	ticker := time.NewTicker(websocketPingInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			room.writeMu.Lock()
			if room.conn != conn {
				room.writeMu.Unlock()
				return
			}
			err := conn.WriteControl(gorilla.PingMessage, nil, time.Now().Add(websocketWriteTimeout))
			room.writeMu.Unlock()
			if err != nil {
				_ = conn.Close()
				return
			}
		case <-done:
			return
		case <-room.done:
			return
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
				configureConn(conn)
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
