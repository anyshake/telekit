package transport_raknet

import "time"

type Option func(*Transport)

// New creates a RakNet transport. RakNet Conn.Write uses the
// ReliableOrdered delivery mode for every application packet.
func New(options ...Option) *Transport {
	t := &Transport{
		MaxMTU:             DefaultMTU,
		MaxTransientErrors: 10,
	}
	for _, option := range options {
		if option != nil {
			option(t)
		}
	}
	return t
}

func WithMaxMTU(mtu uint16) Option {
	return func(t *Transport) { t.MaxMTU = mtu }
}

func WithMaxTransientErrors(max int) Option {
	return func(t *Transport) { t.MaxTransientErrors = max }
}

func WithDisableCookies(disable bool) Option {
	return func(t *Transport) { t.DisableCookies = disable }
}

func WithBlockDuration(duration time.Duration) Option {
	return func(t *Transport) { t.BlockDuration = duration }
}

// WithWritePacing configures the adaptive interval between application
// packets. min and max are clamped by Transport.pacing; window is the target
// number of packets per measured RTT.
func WithWritePacing(min, max time.Duration, window int) Option {
	return func(t *Transport) {
		t.MinWriteInterval = min
		t.MaxWriteInterval = max
		t.PacingWindow = window
	}
}
