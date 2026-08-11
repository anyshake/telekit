package transport_quic

import (
	"time"

	"github.com/anyshake/telekit/transports/transport_quic/congestion/bbr"
	quic "github.com/apernet/quic-go"
)

type Option func(*Transport)

func New(options ...Option) *Transport {
	t := &Transport{Config: defaultConfig()}
	for _, option := range options {
		if option != nil {
			option(t)
		}
	}
	return t
}

func WithConfig(config *quic.Config) Option {
	return func(t *Transport) { t.Config = config }
}

// WithBBRProfile selects the Hysteria/Xray BBR tuning profile. Standard is
// used when this option is omitted.
func WithBBRProfile(profile bbr.Profile) Option {
	return func(t *Transport) { t.bbrProfile = profile }
}

// WithBrutalBandwidth enables Hysteria's fixed-rate sender. The value is in
// bits per second and must only be used when the available bandwidth is known.
// It takes precedence over the BBR profile.
func WithBrutalBandwidth(bitsPerSecond uint64) Option {
	return func(t *Transport) { t.brutalBandwidth = bitsPerSecond }
}

func WithInitialPacketSize(size uint16) Option {
	return func(t *Transport) {
		if t.Config == nil {
			t.Config = defaultConfig()
		}
		t.Config.InitialPacketSize = size
	}
}

func WithMaxIdleTimeout(timeout time.Duration) Option {
	return func(t *Transport) {
		if t.Config == nil {
			t.Config = defaultConfig()
		}
		t.Config.MaxIdleTimeout = timeout
	}
}

func WithReceiveWindows(initialStream, maxStream, initialConnection, maxConnection uint64) Option {
	return func(t *Transport) {
		if t.Config == nil {
			t.Config = defaultConfig()
		}
		t.Config.InitialStreamReceiveWindow = initialStream
		t.Config.MaxStreamReceiveWindow = maxStream
		t.Config.InitialConnectionReceiveWindow = initialConnection
		t.Config.MaxConnectionReceiveWindow = maxConnection
	}
}
