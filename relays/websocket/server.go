package websocket

import (
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

// AllocationRequest is passed to Authorize before a relay allocation is
// created.
type AllocationRequest struct {
	// Path is the HTTP request path used as the relay namespace.
	Path     string
	Session  string
	Token    string
	ID       string
	Ufrag    string
	Network  string
	SourceID string
	TargetID string
}

// ServerConfig configures the WebSocket relay HTTP handler. RelayAddress and
// RelayPort are the fixed address and port peers publish in their ICE relay
// candidates. RelayPort is a logical relay port; all data is carried by the
// WebSocket connections.
type ServerConfig struct {
	RelayAddress net.IP
	RelayPort    uint16

	HandshakeTimeout time.Duration
	PingInterval     time.Duration
	MaxMessageSize   int64
	CheckOrigin      func(*http.Request) bool
	Authorize        func(AllocationRequest) error
}

// Server is a WebSocket-to-WebSocket relay. Each allocation is bound to an
// opaque source endpoint ID and an opaque target endpoint ID.
type Server struct {
	config ServerConfig

	upgrader websocket.Upgrader

	mu          sync.RWMutex
	allocations map[string]*allocation
	byEndpoint  map[string]*allocation
	closed      bool
}

type allocation struct {
	path     string
	session  string
	addr     *net.UDPAddr
	sourceID string
	targetID string
	conn     *websocket.Conn

	writeMu   sync.Mutex
	closeOnce sync.Once
}

// NewServer creates a WebSocket relay HTTP handler.
func NewServer(config ServerConfig) (*Server, error) {
	if config.RelayAddress == nil {
		return nil, errors.New("websocket relay address is nil")
	}
	if config.RelayAddress.To4() == nil && config.RelayAddress.To16() == nil {
		return nil, errors.New("websocket relay address is invalid")
	}
	if config.RelayPort == 0 {
		return nil, errors.New("websocket relay port is required")
	}
	if config.HandshakeTimeout <= 0 {
		config.HandshakeTimeout = defaultHandshakeTimeout
	}
	if config.PingInterval <= 0 {
		config.PingInterval = defaultPingInterval
	}
	if config.MaxMessageSize <= 0 {
		config.MaxMessageSize = defaultMaxMessage
	}

	s := &Server{
		config:      config,
		allocations: make(map[string]*allocation),
		byEndpoint:  make(map[string]*allocation),
	}
	s.upgrader = websocket.Upgrader{CheckOrigin: config.CheckOrigin}

	return s, nil
}

// ServeHTTP implements http.Handler.
func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	conn, err := s.upgrader.Upgrade(w, r, nil)
	if err != nil {
		return
	}
	conn.SetReadLimit(s.config.MaxMessageSize)
	if err := conn.SetReadDeadline(time.Now().Add(s.config.HandshakeTimeout)); err != nil {
		_ = conn.Close()
		return
	}

	messageType, raw, err := conn.ReadMessage()
	if err != nil || messageType != websocket.TextMessage {
		_ = conn.Close()
		return
	}
	var request allocateRequest
	if err := json.Unmarshal(raw, &request); err != nil || request.Type != "allocate" {
		_ = conn.Close()
		return
	}
	request.Path = r.URL.Path
	if request.Session == "" || request.ID == "" {
		_ = writeAllocationError(conn, "session and id are required")
		_ = conn.Close()
		return
	}
	if s.config.Authorize != nil {
		if err := s.config.Authorize(AllocationRequest{
			Path:     request.Path,
			Session:  request.Session,
			Token:    request.Token,
			ID:       request.ID,
			Ufrag:    request.Ufrag,
			Network:  request.Network,
			SourceID: request.SourceID,
			TargetID: request.TargetID,
		}); err != nil {
			_ = writeAllocationError(conn, err.Error())
			_ = conn.Close()
			return
		}
	}

	alloc, err := s.addAllocation(request, conn)
	if err != nil {
		_ = writeAllocationError(conn, err.Error())
		_ = conn.Close()
		return
	}
	defer s.removeAllocation(alloc)
	defer alloc.close()

	_ = conn.SetReadDeadline(time.Time{})
	if err := writeJSON(conn, allocateResponse{
		Type:    "allocated",
		Address: alloc.addr.IP.String(),
		Port:    alloc.addr.Port,
		Network: request.Network,
	}); err != nil {
		return
	}

	for {
		messageType, frame, err := conn.ReadMessage()
		if err != nil {
			return
		}
		if messageType != websocket.BinaryMessage {
			continue
		}
		destination, payload, err := decodeFrame(frame)
		if err != nil {
			return
		}
		s.forward(alloc, destination, payload)
	}
}

func (s *Server) addAllocation(request allocateRequest, conn *websocket.Conn) (*allocation, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil, net.ErrClosed
	}
	if _, exists := s.allocations[request.ID]; exists {
		return nil, errors.New("relay allocation id is already in use")
	}
	if request.SourceID == "" || request.TargetID == "" {
		return nil, errors.New("source and target endpoint IDs are required")
	}
	endpointKey := endpointKey(request.Path, request.Session, request.SourceID)
	if _, exists := s.byEndpoint[endpointKey]; exists {
		return nil, errors.New("source endpoint is already allocated")
	}

	addr := &net.UDPAddr{
		IP:   append(net.IP(nil), s.config.RelayAddress...),
		Port: int(s.config.RelayPort),
	}

	alloc := &allocation{
		path:     request.Path,
		session:  request.Session,
		addr:     addr,
		sourceID: request.SourceID,
		targetID: request.TargetID,
		conn:     conn,
	}
	s.allocations[request.ID] = alloc
	s.byEndpoint[endpointKey] = alloc
	return alloc, nil
}

func (s *Server) removeAllocation(alloc *allocation) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for id, current := range s.allocations {
		if current == alloc {
			delete(s.allocations, id)
		}
	}
	delete(s.byEndpoint, endpointKey(alloc.path, alloc.session, alloc.sourceID))
}

func (s *Server) forward(source *allocation, destination *net.UDPAddr, payload []byte) {
	if destination == nil || !destination.IP.Equal(s.config.RelayAddress) || destination.Port != int(s.config.RelayPort) {
		return
	}

	s.mu.RLock()
	target := s.byEndpoint[endpointKey(source.path, source.session, source.targetID)]
	s.mu.RUnlock()
	if target == nil || target.targetID != source.sourceID {
		return
	}

	frame, err := encodeFrame(source.addr, payload)
	if err != nil {
		return
	}
	target.writeMu.Lock()
	_ = target.conn.WriteMessage(websocket.BinaryMessage, frame)
	target.writeMu.Unlock()
}

// Close stops the relay and closes all active allocations.
func (s *Server) Close() error {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil
	}
	s.closed = true
	allocations := make([]*allocation, 0, len(s.allocations))
	for _, alloc := range s.allocations {
		allocations = append(allocations, alloc)
	}
	s.mu.Unlock()
	for _, alloc := range allocations {
		alloc.close()
	}
	return nil
}

func endpointKey(path, session, endpointID string) string {
	return path + "\x00" + session + "\x00" + endpointID
}

func (a *allocation) close() {
	a.closeOnce.Do(func() { _ = a.conn.Close() })
}

func writeJSON(conn *websocket.Conn, value any) error {
	return conn.WriteJSON(value)
}

func writeAllocationError(conn *websocket.Conn, message string) error {
	return writeJSON(conn, allocateResponse{Type: "error", Error: fmt.Sprint(message)})
}
