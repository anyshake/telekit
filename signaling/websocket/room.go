package websocket

import (
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/anyshake/telekit/signaling"
	"github.com/anyshake/telekit/signaling/websocket/broker"
	gorilla "github.com/gorilla/websocket"
)

func (a *Adapter) ensureRoom(roomID string) (*roomConn, error) {
	if err := signaling.ValidateRoomID(roomID); err != nil {
		return nil, err
	}
	a.mu.Lock()
	if !a.connected {
		a.mu.Unlock()
		return nil, signaling.ErrClosed
	}
	if room := a.rooms[roomID]; room != nil {
		a.mu.Unlock()
		return room, nil
	}
	a.mu.Unlock()

	conn, err := a.dialRoom(roomID)
	if err != nil {
		return nil, err
	}

	a.mu.Lock()
	if !a.connected {
		a.mu.Unlock()
		_ = conn.Close()
		return nil, signaling.ErrClosed
	}
	if room := a.rooms[roomID]; room != nil {
		a.mu.Unlock()
		_ = conn.Close()
		return room, nil
	}
	configureConn(conn)
	room := &roomConn{
		conn:     conn,
		done:     make(chan struct{}),
		handlers: make(map[signaling.MessageType]map[uint64]signaling.Handler),
	}
	a.rooms[roomID] = room
	a.mu.Unlock()
	go a.readLoop(roomID, room, conn)
	if a.onConnect != nil {
		a.onConnect()
	}
	return room, nil
}

func configureConn(conn *gorilla.Conn) {
	conn.SetReadLimit(broker.MaxMessageSize)
	_ = conn.SetReadDeadline(time.Now().Add(websocketPongWait))
	conn.SetPongHandler(func(string) error {
		return conn.SetReadDeadline(time.Now().Add(websocketPongWait))
	})
}

func (a *Adapter) dialRoom(roomID string) (*gorilla.Conn, error) {
	endpoint, err := a.roomURL(roomID)
	if err != nil {
		return nil, err
	}
	conn, response, err := a.dialer.Dial(endpoint, a.headers.Clone())
	if err != nil {
		return nil, wrapHandshakeError(response, err)
	}
	return conn, err
}

func (a *Adapter) roomURL(roomID string) (string, error) {
	u, err := url.Parse(a.baseURL)
	if err != nil {
		return "", err
	}
	u.Path = strings.TrimRight(u.Path, "/") + "/" + url.PathEscape(roomID)
	u.RawPath = ""
	return u.String(), nil
}

func (a *Adapter) roomActive(roomID string, room *roomConn) bool {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.connected && a.rooms[roomID] == room
}

func closeHandshakeResponse(response *http.Response) {
	if response != nil && response.Body != nil {
		_ = response.Body.Close()
	}
}

func wrapHandshakeError(response *http.Response, err error) error {
	if response == nil || response.Body == nil {
		return err
	}
	defer response.Body.Close()
	body, readErr := io.ReadAll(io.LimitReader(response.Body, 4096))
	if readErr != nil || len(body) == 0 {
		return fmt.Errorf("%w: HTTP %s", err, response.Status)
	}
	return fmt.Errorf("%w: HTTP %s: %s", err, response.Status, strings.TrimSpace(string(body)))
}

func (a *Adapter) removeRoom(roomID string, room *roomConn) {
	a.mu.Lock()
	if a.rooms[roomID] == room {
		delete(a.rooms, roomID)
	}
	a.mu.Unlock()
}

func (r *roomConn) close() {
	r.closeOnce.Do(func() {
		close(r.done)
		r.writeMu.Lock()
		if r.conn != nil {
			_ = r.conn.Close()
			r.conn = nil
		}
		r.writeMu.Unlock()
	})
}
