package server

import (
	"crypto/ed25519"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/alphadose/haxmap"
	"github.com/anyshake/telekit/peer"
	"github.com/anyshake/telekit/peer/api"
	"github.com/anyshake/telekit/signaling"
	"github.com/anyshake/telekit/transports"
	lru "github.com/hashicorp/golang-lru/v2"
	"github.com/pion/ice/v4"
	"github.com/pion/webrtc/v4"
	"golang.org/x/time/rate"
)

const (
	DEFAULT_REPLAY_PROTECTION     = 5 * time.Minute
	DEFAULT_LRU_SIZE              = 100000
	MAX_CHUNK_SIZE                = 60000
	DEFAULT_MAX_FRAME_SIZE        = 4 << 20
	DEFAULT_MAX_PENDING_ICE       = 128
	DEFAULT_MAX_PENDING_ICE_BYTES = 256 << 10
	DEFAULT_MAX_CONNECTIONS       = 1024
	DEFAULT_MAX_HANDSHAKES        = 256
	DEFAULT_MAX_BUFFERED          = 256 << 20
)

type Options struct {
	// LRUSize is the capacity of the replay-protection nonce cache.
	LRUSize int
	// ReplayProtection is the retention period for seen handshake nonces.
	ReplayProtection time.Duration
	// ClockSkew is the allowed difference between peer and local handshake clocks.
	ClockSkew time.Duration
	// KeyProvider returns the PSK for the client identity in ClientHello.
	KeyProvider peer.KeyProvider
	// IdentityKey signs ServerHello transcripts. Clients pin the corresponding
	// Ed25519 public key before connecting.
	IdentityKey ed25519.PrivateKey
	// Transport configures one data transport. Nil defaults to raw UDP when
	// Transports is also empty.
	Transport transports.ITransport
	// Transports advertises the data transports accepted after ICE.
	Transports []transports.ITransport
	// EncryptionType selects the frame cipher. Empty uses XChaCha20-Poly1305.
	EncryptionType string
	// EncryptionAAD is additional authenticated data included in each frame.
	EncryptionAAD []byte
	// UseCompression enables zstd compression before encryption.
	UseCompression bool
	// MaxFrameSize is the maximum decoded encrypted transport frame size.
	MaxFrameSize int
	// ReceiveBufferSize is the maximum unread application data retained by Read.
	ReceiveBufferSize int
	// MaxBufferedBytes is the server-wide receive and frame-reassembly budget.
	MaxBufferedBytes int64
	// MaxPendingICE limits the number of ICE candidates buffered per connection.
	MaxPendingICE int
	// MaxPendingICEBytes limits the bytes used by buffered ICE candidates.
	MaxPendingICEBytes int
	// MaxConnections limits the total number of accepted client connections.
	MaxConnections int
	// MaxPendingHandshakes limits authenticated handshakes awaiting transport setup.
	MaxPendingHandshakes int
	// HandshakeTimeout limits the lifetime of a pending handshake.
	HandshakeTimeout time.Duration
	// HelloRateLimit is the sustained ClientHello rate accepted by the server.
	HelloRateLimit float64
	// HelloRateBurst is the maximum initial ClientHello burst.
	HelloRateBurst int
	// OnNewClientJoin is called before a client enters the connection map. Return
	// false to reject the client.
	OnNewClientJoin func(*haxmap.Map[string, *Connection], string) bool
	// OnNewClientReject is called when a client is rejected before connection setup.
	OnNewClientReject func(string, error)
	// OnConnectionFailed is called when an authenticated connection cannot be established.
	OnConnectionFailed func(*Connection, error)
	// OnDisconnected is called after an established connection is lost.
	OnDisconnected func(*Connection)
}

type Connection struct {
	sourceId           string
	codec              *peer.Codec
	handshakeCodec     *peer.Codec
	sessionSalt        []byte
	clientNonce        []byte
	serverNonce        []byte
	clientEphemeralKey []byte
	serverEphemeralKey []byte

	transportConn     net.Conn
	lastTransportRead atomic.Int64
	established       atomic.Bool
	iceAgent          *ice.Agent
	localAddr         peer.Addr
	remoteAddr        peer.Addr
	selectedTransport string
	pendingICEOffer   *transports.ICEDescription
	pendingICE        []webrtc.ICECandidateInit
	pendingICEBytes   int
	stateMu           sync.RWMutex

	writeMu sync.Mutex

	// recvBuf is the TCP-like receive buffer. Use Read() to consume data.
	recvBuf *peer.RecvBuffer

	signalMu       sync.Mutex
	signalSend     atomic.Uint64
	signalRecv     peer.ReplayWindow
	serverId       string
	roomId         string
	deadlineMu     sync.RWMutex
	writeDeadline  time.Time
	owner          *Server
	pendingLease   atomic.Bool
	totalLease     atomic.Bool
	handshakeMu    sync.Mutex
	handshakeTimer *time.Timer
	closeOnce      sync.Once
	closeErr       error
}

type Server struct {
	serverId string
	api      *api.API
	options  *Options

	// defaultCodec is only used
	// for decoding message header
	defaultCodec *peer.Codec
	nonceCache   *lru.Cache[string, int64]

	onOffer     signaling.Subscription
	onHandshake signaling.Subscription

	connections       *haxmap.Map[string, *Connection]
	acceptCh          chan *Connection
	closeCh           chan struct{}
	closeOnce         sync.Once
	roomLeaseKey      string
	bufferBudget      *peer.ByteBudget
	helloLimiter      *rate.Limiter
	totalConnections  atomic.Int64
	pendingHandshakes atomic.Int64
}
