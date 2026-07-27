package api

import (
	"errors"
	"strings"

	"github.com/anyshake/telekit/signaling"
	"github.com/pion/stun/v3"
)

func parseICEURI(raw string) (*stun.URI, error) {
	uri, err := stun.ParseURI(raw)
	if err == nil {
		return uri, nil
	}
	// Accept the WebRTC-style stun://host form used by existing examples.
	if strings.Contains(raw, "://") {
		return stun.ParseURI(strings.Replace(raw, "://", ":", 1))
	}
	return nil, err
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
