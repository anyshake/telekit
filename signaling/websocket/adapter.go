// Package websocket implements signaling over a room-oriented WebSocket
// relay. Each room is one URL path segment below the configured endpoint.
package websocket

import (
	"encoding/json"
	"errors"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/anyshake/telekit/signaling"
	gorilla "github.com/gorilla/websocket"
)

type packet struct {
	Type    signaling.MessageType `json:"type"`
	Payload []byte                `json:"payload"`
}

const maxMessageSize = 16 << 20

type roomConn struct {
	conn *gorilla.Conn

	writeMu  sync.Mutex
	handlerM sync.RWMutex
	handlers map[signaling.MessageType]map[uint64]signaling.Handler
}

type Adapter struct {
	baseURL string
	dialer  *gorilla.Dialer
	headers http.Header

	mu        sync.Mutex
	connected bool
	rooms     map[string]*roomConn
	nextID    atomic.Uint64
}

func (a *Adapter) SignalingID() string { return "websocket:" + a.baseURL }

type Option func(*Adapter)

func WithDialer(dialer *gorilla.Dialer) Option {
	return func(a *Adapter) {
		if dialer != nil {
			a.dialer = dialer
		}
	}
}

// WithHTTPHeader supplies credentials or other metadata to the Broker's
// authorization callback. The header map is cloned by NewAdapter.
func WithHTTPHeader(headers http.Header) Option {
	return func(a *Adapter) {
		a.headers = headers.Clone()
	}
}

func NewAdapter(baseURL string, opts ...Option) (signaling.Adapter, error) {
	u, err := url.Parse(baseURL)
	if err != nil {
		return nil, err
	}
	if u.Scheme != "ws" && u.Scheme != "wss" {
		return nil, errors.New("WebSocket URL must use ws or wss")
	}
	a := &Adapter{
		baseURL: strings.TrimRight(baseURL, "/"),
		dialer:  gorilla.DefaultDialer,
		headers: make(http.Header),
		rooms:   make(map[string]*roomConn),
	}
	for _, opt := range opts {
		opt(a)
	}
	return a, nil
}

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
		_ = room.conn.Close()
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
	return room.conn.WriteMessage(gorilla.TextMessage, data)
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

func (a *Adapter) ensureRoom(roomID string) (*roomConn, error) {
	if err := signaling.ValidateRoomID(roomID); err != nil {
		return nil, err
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if !a.connected {
		return nil, signaling.ErrClosed
	}
	if room := a.rooms[roomID]; room != nil {
		return room, nil
	}
	conn, _, err := a.dialer.Dial(a.baseURL+"/"+url.PathEscape(roomID), a.headers.Clone())
	if err != nil {
		return nil, err
	}
	conn.SetReadLimit(maxMessageSize)
	room := &roomConn{conn: conn, handlers: make(map[signaling.MessageType]map[uint64]signaling.Handler)}
	a.rooms[roomID] = room
	go a.readLoop(roomID, room)
	return room, nil
}

func (a *Adapter) readLoop(roomID string, room *roomConn) {
	defer func() {
		_ = room.conn.Close()
		a.mu.Lock()
		if a.rooms[roomID] == room {
			delete(a.rooms, roomID)
		}
		a.mu.Unlock()
	}()
	for {
		_, data, err := room.conn.ReadMessage()
		if err != nil {
			return
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
