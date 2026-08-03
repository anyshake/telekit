package api

import (
	"errors"
	"fmt"

	relayws "github.com/anyshake/telekit/relays/websocket"
	"github.com/anyshake/telekit/signaling"
	"github.com/pion/stun/v3"
)

type Option func(*API) error

type webSocketRelayConfig struct {
	ServerID string
	URL      string
	Token    string
}

type API struct {
	RoomId          string
	SignalingServer signaling.IAdapter
	// ICEURLs is used by the standalone Pion ICE agent after signaling.
	ICEURLs        []*stun.URI
	webSocketRelay *webSocketRelayConfig
}

// WebSocketRelayProvider creates a relay candidate allocator for one
// direction of a client/server relay session.
func (a *API) WebSocketRelayProvider(localID, peerID string) (*relayws.Provider, error) {
	if a == nil || a.webSocketRelay == nil {
		return nil, nil
	}
	config := a.webSocketRelay
	if localID == "" || peerID == "" {
		return nil, errors.New("websocket relay endpoint IDs are required")
	}
	var clientID string
	switch {
	case localID == config.ServerID:
		clientID = peerID
	case peerID == config.ServerID:
		clientID = localID
	default:
		return nil, fmt.Errorf("websocket relay endpoint pair does not contain server ID %q", config.ServerID)
	}

	provider, err := relayws.NewProvider(relayws.ProviderConfig{
		URL:     config.URL,
		Session: relayws.RelaySessionID(a.RoomId, clientID),
		Token:   config.Token,
		LocalID: localID,
		PeerID:  peerID,
	})
	if err != nil {
		return nil, err
	}
	return provider, nil
}
