package api

import (
	"errors"

	"github.com/anyshake/telekit/signaling"
	"github.com/pion/stun/v3"
	"github.com/pion/webrtc/v4"
	"github.com/samber/lo"
)

func NewAPI(roomId string, signalingServer signaling.IAdapter, opts ...Option) (*API, error) {
	if roomId == "" {
		return nil, errors.New("room ID is not set")
	}
	if err := signaling.ValidateRoomID(roomId); err != nil {
		return nil, err
	}
	if signalingServer == nil {
		return nil, errors.New("signaling server adapter is not set")
	}

	var se webrtc.SettingEngine
	se.SetNetworkTypes([]webrtc.NetworkType{
		webrtc.NetworkTypeUDP6,
		webrtc.NetworkTypeUDP4,
		webrtc.NetworkTypeTCP6,
		webrtc.NetworkTypeTCP4,
	})
	if err := se.SetEphemeralUDPPortRange(1024, 65535); err != nil {
		return nil, err
	}

	api := &API{
		DataChannel:     DEFAULT_DATACHANNEL,
		SignalingServer: signalingServer,
		RoomId:          roomId,
		WebRTCAPI:       webrtc.NewAPI(webrtc.WithSettingEngine(se)),
	}
	for _, opt := range opts {
		if err := opt(api); err != nil {
			return nil, err
		}
	}

	return api, nil
}

func WithDataChannel(channel string) Option {
	return func(api *API) error {
		api.DataChannel = channel
		return nil
	}
}

func WithSTUNServer(urls ...string) Option {
	return func(api *API) error {
		var stuns []webrtc.ICEServer
		lo.ForEach(urls, func(url string, index int) {
			s, err := newICEServer(url)
			if err != nil {
				return
			}
			if s.scheme == stun.SchemeTypeSTUN || s.scheme == stun.SchemeTypeSTUNS {
				stuns = append(stuns, s.ToPionFormat())
			}
		})
		if len(stuns) == 0 {
			return errors.New("no valid STUN server specified")
		}
		api.WebRTCConfig.ICEServers = append(api.WebRTCConfig.ICEServers, webrtc.ICEServer{
			URLs: lo.Map(stuns, func(s webrtc.ICEServer, index int) string { return s.URLs[0] }),
		})
		return nil
	}
}

func WithTURNServer(urls ...string) Option {
	return func(api *API) error {
		var turns []*ICEServer
		lo.ForEach(urls, func(url string, index int) {
			s, err := newICEServer(url)
			if err != nil {
				return
			}
			if s.scheme == stun.SchemeTypeTURN || s.scheme == stun.SchemeTypeTURNS {
				turns = append(turns, s)
			}
		})
		if len(turns) == 0 {
			return errors.New("no valid TURN server specified")
		}
		for _, s := range turns {
			api.WebRTCConfig.ICEServers = append(api.WebRTCConfig.ICEServers, s.ToPionFormat())
		}
		return nil
	}
}

func WithICECandidatePoolSize(size uint8) Option {
	return func(cfg *API) error {
		cfg.WebRTCConfig.ICECandidatePoolSize = size
		return nil
	}
}

func WithICETransportPolicy(policy webrtc.ICETransportPolicy) Option {
	return func(cfg *API) error {
		cfg.WebRTCConfig.ICETransportPolicy = policy
		return nil
	}
}
