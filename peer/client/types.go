package client

import (
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/anyshake/telekit/peer"
	"github.com/anyshake/telekit/peer/api"
	"github.com/anyshake/telekit/transports"
	"github.com/pion/ice/v4"
	"github.com/pion/webrtc/v4"
)

const (
	DEFAULT_TIMEOUT               = 30 * time.Second
	DEFAULT_MAX_FRAME_SIZE        = 4 << 20
	DEFAULT_MAX_PENDING_ICE       = 128
	DEFAULT_MAX_PENDING_ICE_BYTES = 256 << 10
	transportKeepaliveInterval    = 5 * time.Second
	transportKeepaliveTimeout     = 15 * time.Second
)

type Options struct {
	// TimeFunc returns the current time used to generate handshake timestamps.
	TimeFunc func() time.Time
	// Timeout is the maximum time to wait for a connection to be established.
	Timeout time.Duration
	// Transport selects the data transport negotiated after ICE. Nil defaults to
	// the raw UDP transport.
	Transport transports.ITransport
	// UseCompression enables zstd compression before encryption.
	UseCompression bool
	// MaxFrameSize is the maximum decoded encrypted transport frame size.
	MaxFrameSize int
	// ReceiveBufferSize is the maximum unread application data retained by Read.
	ReceiveBufferSize int
	// MaxPendingICE limits the number of ICE candidates buffered per connection.
	MaxPendingICE int
	// MaxPendingICEBytes limits the bytes used by buffered ICE candidates.
	MaxPendingICEBytes int
	// EncryptionAAD is additional authenticated data included in each frame.
	EncryptionAAD []byte
	// EncryptionType selects the frame cipher. Empty uses XChaCha20-Poly1305.
	EncryptionType string
	// OnClientHello is called after ClientHello is sent.
	OnClientHello func(*Client)
	// OnServerHello is called after a valid ServerHello is received.
	OnServerHello func(*Client)
	// OnConnectionFailed is called when connection establishment fails.
	OnConnectionFailed func(*Client, error)
	// OnDisconnected is called after an established connection is lost.
	OnDisconnected func(*Client)
}

type Client struct {
	clientId string
	serverId string
	psk      peer.PreSharedKey
	api      *api.API
	options  *Options
	codec    *peer.Codec

	signalMu   sync.Mutex
	signalSend atomic.Uint64
	signalRecv peer.ReplayWindow
	// Kept only for decoding compatibility with pre-ICE callers. New handshakes
	// exchange a complete ICE description and never use this queue.
	pendingICE      []webrtc.ICECandidateInit
	pendingICEBytes int

	transportConn     net.Conn
	lastTransportRead atomic.Int64
	iceAgent          *ice.Agent
	localAddr         peer.Addr
	remoteAddr        peer.Addr
	stateMu           sync.RWMutex

	writeMu sync.Mutex

	// recvBuf is the TCP-like receive buffer. Use Read() to consume data.
	// Replaced with a fresh buffer on each ConnectWithContext call.
	recvBuf atomic.Pointer[peer.RecvBuffer]

	mu            sync.RWMutex
	connected     bool
	connecting    bool
	deadlineMu    sync.RWMutex
	writeDeadline time.Time
}
