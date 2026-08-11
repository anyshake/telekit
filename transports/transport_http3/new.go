package transport_http3

import (
	"crypto/tls"
	"strings"
	"time"

	"github.com/anyshake/telekit/transports/transport_quic/congestion/bbr"
	quic "github.com/apernet/quic-go"
)

const defaultPath = "/"

// Option configures the HTTP/3 transport.
type Option func(*Transport)

// Transport carries Telekit frames in the body of an authenticated HTTP/3
// POST request. Requests without a valid Telekit token are handled by the
// configured fallback handler instead.
type Transport struct {
	Config    *quic.Config
	TLSConfig *tls.Config

	// FallbackURL is the upstream website used for unauthenticated HTTP/3
	// requests. It must be an absolute http:// or https:// URL.
	FallbackURL string
	// ServerName is the SNI and request host used by the client. If empty, the
	// fallback host or www.microsoft.com is used.
	ServerName string
	// Path is the authenticated endpoint path. The default is "/".
	Path string

	bbrProfile      bbr.Profile
	brutalBandwidth uint64
}

func New(options ...Option) *Transport {
	t := &Transport{
		Config: defaultConfig(),
		Path:   defaultPath,
	}
	for _, option := range options {
		if option != nil {
			option(t)
		}
	}
	if strings.TrimSpace(t.Path) == "" {
		t.Path = defaultPath
	}
	return t
}

func WithConfig(config *quic.Config) Option {
	return func(t *Transport) { t.Config = config }
}

func WithTLSConfig(config *tls.Config) Option {
	return func(t *Transport) { t.TLSConfig = config }
}

func WithFallbackURL(rawURL string) Option {
	return func(t *Transport) { t.FallbackURL = rawURL }
}

func WithServerName(name string) Option {
	return func(t *Transport) { t.ServerName = name }
}

func WithPath(path string) Option {
	return func(t *Transport) { t.Path = path }
}

func WithMaxIdleTimeout(timeout time.Duration) Option {
	return func(t *Transport) {
		if t.Config == nil {
			t.Config = defaultConfig()
		}
		t.Config.MaxIdleTimeout = timeout
	}
}

// WithBBRProfile selects the same Hysteria/Xray BBR tuning profile as the
// regular QUIC transport. Empty uses the standard profile.
func WithBBRProfile(profile bbr.Profile) Option {
	return func(t *Transport) { t.bbrProfile = profile }
}

// WithBrutalBandwidth enables the same fixed-rate sender as the regular QUIC
// transport. The value is in bits per second and takes precedence over BBR.
func WithBrutalBandwidth(bitsPerSecond uint64) Option {
	return func(t *Transport) { t.brutalBandwidth = bitsPerSecond }
}

func defaultConfig() *quic.Config {
	return &quic.Config{
		InitialPacketSize:              1200,
		DisablePathMTUDiscovery:        true,
		HandshakeIdleTimeout:           15 * time.Second,
		MaxIdleTimeout:                 5 * time.Minute,
		InitialStreamReceiveWindow:     4 << 20,
		MaxStreamReceiveWindow:         32 << 20,
		InitialConnectionReceiveWindow: 8 << 20,
		MaxConnectionReceiveWindow:     64 << 20,
		// HTTP/3 does not need QUIC DATAGRAM for the Telekit byte stream.
		EnableDatagrams:    false,
		DisablePathManager: true,
	}
}
