package broker

import (
	"net"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/alphadose/haxmap"
	"github.com/anyshake/telekit/signaling"
	gorilla "github.com/gorilla/websocket"
	"golang.org/x/time/rate"
)

const (
	defaultConnectionsPerRoom  = 256
	defaultConnectionsPerIP    = 32
	defaultClientQueueMessages = 64
	defaultClientQueueBytes    = 16 << 20
	defaultClientMessageRate   = 200
	defaultClientMessageBurst  = 400
	defaultWriteTimeout        = 10 * time.Second
	defaultPongWait            = 60 * time.Second
)

const MaxMessageSize = 16 << 20

// Broker is a room-scoped WebSocket relay. Authorization is deny-by-default;
// production callers must supply an ACL with WithAuthorization.
type Broker struct {
	upgrader gorilla.Upgrader

	roomMu   sync.Mutex
	countsMu sync.Mutex

	rooms      *haxmap.Map[string, *roomState]
	ipCounts   *haxmap.Map[string, int]
	roomCounts *haxmap.Map[string, int]

	authorize          func(*http.Request, string) bool
	connectionsPerRoom int
	connectionsPerIP   int
	queueMessages      int
	queueBytes         int64
	messageRate        rate.Limit
	messageBurst       int
	writeTimeout       time.Duration
	pongWait           time.Duration
}

type roomState struct {
	mu      sync.Mutex
	closed  bool
	clients map[*brokerClient]struct{}
}

type outboundMessage struct {
	typ  int
	data []byte
}

type brokerClient struct {
	conn          *gorilla.Conn
	send          chan outboundMessage
	done          chan struct{}
	closeOnce     sync.Once
	queuedBytes   atomic.Int64
	maxQueueBytes int64
	limiter       *rate.Limiter
	writeTimeout  time.Duration
	pongWait      time.Duration
}

type BrokerOption func(*Broker)

func WithOriginCheck(check func(*http.Request) bool) BrokerOption {
	return func(b *Broker) { b.upgrader.CheckOrigin = check }
}

// WithAuthorization installs the room ACL. It is called before WebSocket
// upgrade, so rejected requests consume no connection slot.
func WithAuthorization(authorize func(*http.Request, string) bool) BrokerOption {
	return func(b *Broker) { b.authorize = authorize }
}

func WithConnectionLimits(perRoom, perIP int) BrokerOption {
	return func(b *Broker) {
		if perRoom > 0 {
			b.connectionsPerRoom = perRoom
		}
		if perIP > 0 {
			b.connectionsPerIP = perIP
		}
	}
}

func WithClientQueueLimits(messages, bytes int) BrokerOption {
	return func(b *Broker) {
		if messages > 0 {
			b.queueMessages = messages
		}
		if bytes > 0 {
			b.queueBytes = int64(bytes)
		}
	}
}

func WithClientRateLimit(messagesPerSecond float64, burst int) BrokerOption {
	return func(b *Broker) {
		if messagesPerSecond > 0 {
			b.messageRate = rate.Limit(messagesPerSecond)
		}
		if burst > 0 {
			b.messageBurst = burst
		}
	}
}

func WithBrokerTimeouts(pongWait, writeTimeout time.Duration) BrokerOption {
	return func(b *Broker) {
		if pongWait > 0 {
			b.pongWait = pongWait
		}
		if writeTimeout > 0 {
			b.writeTimeout = writeTimeout
		}
	}
}

func NewBroker(opts ...BrokerOption) *Broker {
	b := &Broker{
		upgrader:           gorilla.Upgrader{},
		rooms:              haxmap.New[string, *roomState](),
		ipCounts:           haxmap.New[string, int](),
		roomCounts:         haxmap.New[string, int](),
		connectionsPerRoom: defaultConnectionsPerRoom,
		connectionsPerIP:   defaultConnectionsPerIP,
		queueMessages:      defaultClientQueueMessages,
		queueBytes:         defaultClientQueueBytes,
		messageRate:        defaultClientMessageRate,
		messageBurst:       defaultClientMessageBurst,
		writeTimeout:       defaultWriteTimeout,
		pongWait:           defaultPongWait,
	}
	for _, opt := range opts {
		opt(b)
	}
	return b
}

func (b *Broker) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	roomID := strings.Trim(r.URL.Path, "/")
	if err := signaling.ValidateRoomID(roomID); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if b.authorize == nil || !b.authorize(r, roomID) {
		http.Error(w, "WebSocket room access denied", http.StatusForbidden)
		return
	}
	ip := remoteIP(r.RemoteAddr)
	if !b.reserve(roomID, ip) {
		http.Error(w, "WebSocket connection limit reached", http.StatusTooManyRequests)
		return
	}
	defer b.release(roomID, ip)

	conn, err := b.upgrader.Upgrade(w, r, nil)
	if err != nil {
		return
	}
	client := &brokerClient{
		conn:          conn,
		send:          make(chan outboundMessage, b.queueMessages),
		done:          make(chan struct{}),
		maxQueueBytes: b.queueBytes,
		limiter:       rate.NewLimiter(b.messageRate, b.messageBurst),
		writeTimeout:  b.writeTimeout,
		pongWait:      b.pongWait,
	}
	conn.SetReadLimit(MaxMessageSize)
	_ = conn.SetReadDeadline(time.Now().Add(b.pongWait))
	conn.SetPongHandler(func(string) error {
		return conn.SetReadDeadline(time.Now().Add(b.pongWait))
	})

	room, err := b.ensureRoom(roomID)
	if err != nil {
		_ = conn.Close()
		return
	}
	if !room.add(client) {
		b.replaceRoom(roomID, room)
		room, err = b.ensureRoom(roomID)
		if err != nil || !room.add(client) {
			_ = conn.Close()
			return
		}
	}

	go client.writeLoop()
	defer func() {
		b.remove(roomID, room, client)
		client.close()
	}()

	for {
		messageType, data, err := conn.ReadMessage()
		if err != nil {
			return
		}
		_ = conn.SetReadDeadline(time.Now().Add(b.pongWait))
		if !client.limiter.Allow() {
			return
		}
		b.broadcast(roomID, messageType, data)
	}
}

func remoteIP(remoteAddr string) string {
	host, _, err := net.SplitHostPort(remoteAddr)
	if err == nil {
		return host
	}
	return remoteAddr
}

func (b *Broker) ensureRoom(roomID string) (*roomState, error) {
	if err := signaling.ValidateRoomID(roomID); err != nil {
		return nil, err
	}
	b.roomMu.Lock()
	defer b.roomMu.Unlock()
	if room, ok := b.rooms.Get(roomID); ok {
		return room, nil
	}
	room := newRoomState()
	b.rooms.Set(roomID, room)
	return room, nil
}

func (b *Broker) replaceRoom(roomID string, room *roomState) {
	b.roomMu.Lock()
	if current, ok := b.rooms.Get(roomID); ok && current == room {
		b.rooms.Del(roomID)
	}
	b.roomMu.Unlock()
}

func newRoomState() *roomState {
	return &roomState{clients: make(map[*brokerClient]struct{})}
}

func (r *roomState) add(client *brokerClient) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return false
	}
	r.clients[client] = struct{}{}
	return true
}

func (r *roomState) remove(client *brokerClient) (empty bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.clients, client)
	empty = len(r.clients) == 0
	if empty {
		r.closed = true
	}
	return empty
}

func (b *Broker) remove(roomID string, room *roomState, client *brokerClient) {
	if !room.remove(client) {
		return
	}
	b.roomMu.Lock()
	if current, ok := b.rooms.Get(roomID); ok && current == room {
		b.rooms.Del(roomID)
	}
	b.roomMu.Unlock()
}

func (b *Broker) broadcast(roomID string, messageType int, data []byte) {
	room, ok := b.rooms.Get(roomID)
	if !ok {
		return
	}
	room.mu.Lock()
	clients := make([]*brokerClient, 0, len(room.clients))
	for client := range room.clients {
		clients = append(clients, client)
	}
	room.mu.Unlock()

	payload := append([]byte(nil), data...)
	for _, client := range clients {
		if !client.enqueue(outboundMessage{typ: messageType, data: payload}) {
			client.close()
		}
	}
}

func (b *Broker) reserve(roomID, ip string) bool {
	b.countsMu.Lock()
	defer b.countsMu.Unlock()
	roomCount, _ := b.roomCounts.Get(roomID)
	ipCount, _ := b.ipCounts.Get(ip)
	if roomCount >= b.connectionsPerRoom || ipCount >= b.connectionsPerIP {
		return false
	}
	b.roomCounts.Set(roomID, roomCount+1)
	b.ipCounts.Set(ip, ipCount+1)
	return true
}

func (b *Broker) release(roomID, ip string) {
	b.countsMu.Lock()
	defer b.countsMu.Unlock()
	if ipCount, ok := b.ipCounts.Get(ip); ok {
		if ipCount <= 1 {
			b.ipCounts.Del(ip)
		} else {
			b.ipCounts.Set(ip, ipCount-1)
		}
	}
	if roomCount, ok := b.roomCounts.Get(roomID); ok {
		if roomCount <= 1 {
			b.roomCounts.Del(roomID)
		} else {
			b.roomCounts.Set(roomID, roomCount-1)
		}
	}
}

func (c *brokerClient) enqueue(message outboundMessage) bool {
	size := int64(len(message.data))
	for {
		used := c.queuedBytes.Load()
		if size > c.maxQueueBytes-used {
			return false
		}
		if c.queuedBytes.CompareAndSwap(used, used+size) {
			break
		}
	}
	select {
	case c.send <- message:
		return true
	case <-c.done:
		c.queuedBytes.Add(-size)
		return false
	default:
		c.queuedBytes.Add(-size)
		return false
	}
}

func (c *brokerClient) writeLoop() {
	pingEvery := c.pongWait / 2
	if pingEvery <= 0 {
		pingEvery = time.Second
	}
	ticker := time.NewTicker(pingEvery)
	defer ticker.Stop()
	for {
		select {
		case <-c.done:
			return
		case message := <-c.send:
			_ = c.conn.SetWriteDeadline(time.Now().Add(c.writeTimeout))
			err := c.conn.WriteMessage(message.typ, message.data)
			c.queuedBytes.Add(-int64(len(message.data)))
			if err != nil {
				c.close()
				return
			}
		case <-ticker.C:
			deadline := time.Now().Add(c.writeTimeout)
			if err := c.conn.WriteControl(gorilla.PingMessage, nil, deadline); err != nil {
				c.close()
				return
			}
		}
	}
}

func (c *brokerClient) close() {
	c.closeOnce.Do(func() {
		close(c.done)
		_ = c.conn.Close()
	})
}
