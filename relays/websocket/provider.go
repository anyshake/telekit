package websocket

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"time"

	"github.com/google/uuid"
	"github.com/gorilla/websocket"
	"github.com/pion/ice/v4"
)

const (
	defaultHandshakeTimeout = 10 * time.Second
	defaultPingInterval     = 20 * time.Second
	DefaultRelayPathPrefix  = "/relay"
)

// ProviderConfig configures a WebSocket relay candidate provider.
// LocalID and PeerID identify the two endpoints inside a relay session. They
// are opaque routing identifiers and are not required to be IP addresses.
type ProviderConfig struct {
	URL     string
	Session string
	// Token is optional and is forwarded verbatim to the relay server.
	Token   string
	LocalID string
	PeerID  string
	Network string
	Headers http.Header
	Dialer  *websocket.Dialer

	HandshakeTimeout time.Duration
	PingInterval     time.Duration
}

// Provider allocates WebSocket-backed relay candidates for an ICE agent.
type Provider struct {
	config ProviderConfig
}

// NewProvider creates a WebSocket relay candidate provider.
func NewProvider(config ProviderConfig) (*Provider, error) {
	if config.URL == "" {
		return nil, errors.New("websocket relay URL is empty")
	}
	if config.Session == "" {
		return nil, errors.New("websocket relay session is empty")
	}
	if config.LocalID == "" || config.PeerID == "" {
		return nil, errors.New("websocket relay local and peer IDs are required")
	}
	if config.Network == "" {
		config.Network = "udp4"
	}
	if config.Network != "udp4" && config.Network != "udp6" {
		return nil, fmt.Errorf("unsupported websocket relay network %q", config.Network)
	}
	if config.HandshakeTimeout <= 0 {
		config.HandshakeTimeout = defaultHandshakeTimeout
	}
	if config.PingInterval <= 0 {
		config.PingInterval = defaultPingInterval
	}
	if config.Dialer == nil {
		config.Dialer = websocket.DefaultDialer
	}
	config.Headers = config.Headers.Clone()

	return &Provider{config: config}, nil
}

// AllocateCandidate allocates a relay candidate and its packet connection.
func (p *Provider) AllocateCandidate(ctx context.Context, ufrag string) (*ice.CandidateRelay, net.PacketConn, error) {
	conn, response, err := p.config.Dialer.DialContext(ctx, p.config.URL, p.config.Headers.Clone())
	if err != nil {
		closeHandshakeResponse(response)
		return nil, nil, err
	}

	packetConn, err := p.allocate(ctx, conn, ufrag)
	if err != nil {
		_ = conn.Close()
		return nil, nil, err
	}
	localAddr, ok := packetConn.LocalAddr().(*net.UDPAddr)
	if !ok || localAddr == nil || localAddr.IP == nil || localAddr.Port < 1 || localAddr.Port > 65535 {
		_ = packetConn.Close()
		return nil, nil, errors.New("websocket relay returned an invalid local address")
	}

	candidate, err := ice.NewCandidateRelay(&ice.CandidateRelayConfig{
		Network:       p.config.Network,
		Address:       localAddr.IP.String(),
		Port:          localAddr.Port,
		Component:     ice.ComponentRTP,
		RelayProtocol: "websocket",
	})
	if err != nil {
		_ = packetConn.Close()
		return nil, nil, err
	}

	return candidate, packetConn, nil
}

type allocateRequest struct {
	Path     string `json:"-"`
	Type     string `json:"type"`
	Session  string `json:"session"`
	Token    string `json:"token,omitempty"`
	ID       string `json:"id"`
	Ufrag    string `json:"ufrag"`
	Network  string `json:"network"`
	SourceID string `json:"source_id"`
	TargetID string `json:"target_id"`
}

type allocateResponse struct {
	Type    string `json:"type"`
	Address string `json:"address,omitempty"`
	Port    int    `json:"port,omitempty"`
	Network string `json:"network,omitempty"`
	Error   string `json:"error,omitempty"`
}

func (p *Provider) allocate(ctx context.Context, conn *websocket.Conn, ufrag string) (*packetConn, error) {
	deadline := time.Now().Add(p.config.HandshakeTimeout)
	if ctxDeadline, ok := ctx.Deadline(); ok && ctxDeadline.Before(deadline) {
		deadline = ctxDeadline
	}
	_ = conn.SetReadDeadline(deadline)

	request := allocateRequest{
		Type:     "allocate",
		Session:  p.config.Session,
		Token:    p.config.Token,
		ID:       uuid.NewString(),
		Ufrag:    ufrag,
		Network:  p.config.Network,
		SourceID: p.config.LocalID,
		TargetID: p.config.PeerID,
	}
	if err := conn.WriteJSON(request); err != nil {
		return nil, err
	}

	messageType, raw, err := conn.ReadMessage()
	if err != nil {
		return nil, err
	}
	if messageType != websocket.TextMessage {
		return nil, errors.New("websocket relay allocation response is not text")
	}
	var response allocateResponse
	if err := json.Unmarshal(raw, &response); err != nil {
		return nil, fmt.Errorf("decode websocket relay allocation response: %w", err)
	}
	if response.Type != "allocated" {
		if response.Error == "" {
			response.Error = "relay allocation failed"
		}
		return nil, errors.New(response.Error)
	}
	ip := net.ParseIP(response.Address)
	if ip == nil || response.Port < 1 || response.Port > 65535 {
		return nil, errors.New("websocket relay returned an invalid address")
	}
	if response.Network != "" {
		if response.Network != p.config.Network {
			return nil, fmt.Errorf("websocket relay network mismatch: %s", response.Network)
		}
	}

	_ = conn.SetReadDeadline(time.Time{})
	return newPacketConn(conn, &net.UDPAddr{IP: ip, Port: response.Port}, p.config.PingInterval), nil
}
