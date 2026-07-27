package transport_quic

import (
	"time"

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

func WithInitialPacketSize(size uint16) Option {
	return func(t *Transport) {
		if t.Config == nil {
			t.Config = defaultConfig()
		}
		t.Config.InitialPacketSize = size
	}
}

func WithKeepAlivePeriod(period time.Duration) Option {
	return func(t *Transport) {
		if t.Config == nil {
			t.Config = defaultConfig()
		}
		t.Config.KeepAlivePeriod = period
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
