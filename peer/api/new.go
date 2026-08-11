package api

import (
	"errors"
	"net"
	"net/url"
	"strings"

	relayws "github.com/anyshake/telekit/relays/websocket"
	"github.com/anyshake/telekit/signaling"
	"github.com/pion/stun/v3"
)

func parseICEURI(raw string) (*stun.URI, error) {
	uri, err := stun.ParseURI(raw)
	if err == nil {
		return uri, nil
	}
	// Accept authority-style URLs such as turn://user:pass@host:port.
	if !strings.Contains(raw, "://") {
		return nil, err
	}

	parsed, parseErr := url.Parse(raw)
	if parseErr != nil {
		return nil, err
	}

	// Keep the legacy stun://host behavior for examples without credentials.
	if parsed.User == nil {
		return stun.ParseURI(strings.Replace(raw, "://", ":", 1))
	}

	if parsed.Path != "" && parsed.Path != "/" {
		return nil, err
	}

	host := parsed.Hostname()
	if host == "" {
		return nil, err
	}

	normalized := parsed.Scheme + ":"
	if port := parsed.Port(); port != "" {
		normalized += net.JoinHostPort(host, port)
	} else {
		normalized += host
	}
	if parsed.RawQuery != "" {
		normalized += "?" + parsed.RawQuery
	}

	uri, err = stun.ParseURI(normalized)
	if err != nil {
		return nil, err
	}
	uri.Username = parsed.User.Username()
	if password, ok := parsed.User.Password(); ok {
		uri.Password = password
	}
	return uri, nil
}

func NewAPI(roomID string, signalingServer signaling.IAdapter, opts ...Option) (*API, error) {
	if roomID == "" {
		return nil, errors.New("room ID is not set")
	}
	if err := signaling.ValidateRoomID(roomID); err != nil {
		return nil, err
	}
	if signalingServer == nil {
		return nil, errors.New("signaling server adapter is not set")
	}
	result := &API{RoomId: roomID, SignalingServer: signalingServer}
	for _, opt := range opts {
		if err := opt(result); err != nil {
			return nil, err
		}
	}
	return result, nil
}

func WithSTUNServer(urls ...string) Option {
	return func(api *API) error {
		var valid []*stun.URI
		for _, raw := range urls {
			uri, err := parseICEURI(raw)
			if err != nil || (uri.Scheme != stun.SchemeTypeSTUN && uri.Scheme != stun.SchemeTypeSTUNS) {
				continue
			}
			valid = append(valid, uri)
		}
		if len(valid) == 0 {
			return errors.New("no valid STUN server specified")
		}
		api.ICEURLs = append(api.ICEURLs, valid...)
		return nil
	}
}

func WithTURNServer(urls ...string) Option {
	return func(api *API) error {
		var valid []*stun.URI
		for _, raw := range urls {
			uri, err := parseICEURI(raw)
			if err != nil || (uri.Scheme != stun.SchemeTypeTURN && uri.Scheme != stun.SchemeTypeTURNS) {
				continue
			}
			valid = append(valid, uri)
		}
		if len(valid) == 0 {
			return errors.New("no valid TURN server specified")
		}
		api.ICEURLs = append(api.ICEURLs, valid...)
		return nil
	}
}

// WithWebSocketRelayServer configures a WebSocket relay as an additional ICE
// relay candidate source. Direct host/srflx candidates and TURN remain
// enabled unless the caller overrides candidate types through ICE options.
// Token may be empty; the relay server's Authorize policy decides whether it
// is required.
func WithWebSocketRelayServer(relayBaseURL, serverID, token string) Option {
	return func(api *API) error {
		if serverID == "" {
			return errors.New("websocket relay server ID is empty")
		}
		endpoint, err := relayws.EndpointURL(relayBaseURL, serverID)
		if err != nil {
			return err
		}
		api.webSocketRelay = &webSocketRelayConfig{
			ServerID: serverID,
			URL:      endpoint,
			Token:    token,
		}
		return nil
	}
}
