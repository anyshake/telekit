package server

import (
	"bytes"
	"crypto/ed25519"
	"sync"
	"sync/atomic"
	"time"

	"github.com/alphadose/haxmap"
	"github.com/anyshake/telekit/peer"
	"github.com/anyshake/telekit/peer/api"
	"github.com/anyshake/telekit/signaling"
	lru "github.com/hashicorp/golang-lru/v2"
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
	DEFAULT_CALLBACK_WORKERS      = 4
	DEFAULT_CALLBACK_QUEUE        = 128
	DEFAULT_MAX_SEND_BUFFER       = 1 << 20
)

type Options struct {
	LRUSize          int
	ReplayProtection time.Duration
	ClockSkew        time.Duration
	KeyProvider      peer.KeyProvider
	// IdentityKey signs ServerHello transcripts. Clients pin the corresponding
	// Ed25519 public key before connecting.
	IdentityKey          ed25519.PrivateKey
	EncryptionType       string
	EncryptionAAD        []byte
	UseCompression       bool
	MaxFrameSize         int
	ReceiveBufferSize    int
	MaxSendBufferSize    int
	MaxBufferedBytes     int64
	MaxPendingICE        int
	MaxPendingICEBytes   int
	MaxConnections       int
	MaxPendingHandshakes int
	HandshakeTimeout     time.Duration
	HelloRateLimit       float64
	HelloRateBurst       int
	CallbackWorkers      int
	CallbackQueueSize    int
	// ReceiveEventsOnly delivers incoming application data only through
	// OnDataChannelMessage. It avoids retaining a duplicate stream copy when
	// the application does not use net.Conn.Read. Event-only connections are
	// not queued for Accept; lifecycle is delivered through callbacks.
	ReceiveEventsOnly    bool
	OnNewClientJoin      func(*haxmap.Map[string, *Connection], string) bool
	OnNewClientReject    func(string, error)
	OnClientOffer        func(*Connection, *webrtc.SessionDescription)
	OnAnswerSent         func(*Connection, *webrtc.SessionDescription)
	OnICECandidateSent   func(*Connection, webrtc.ICECandidateInit)
	OnConnectionFailed   func(*Connection, error)
	OnDisconnected       func(*Connection)
	OnDataChannelOpen    func(*Connection)
	OnDataChannelClose   func(*Connection)
	OnDataChannelMessage func(*Connection, []byte)
	OnDataChannelError   func(*Connection, error)
}

type dataChunk struct {
	mu          sync.Mutex
	expectedLen uint64
	recvBuffer  bytes.Buffer
	reserved    int
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

	pc      *webrtc.PeerConnection
	dc      *webrtc.DataChannel
	stateMu sync.RWMutex

	dataChunkBuf dataChunk
	writeMu      sync.Mutex

	// recvBuf is the TCP-like receive buffer. Use Read() to consume data.
	recvBuf *peer.RecvBuffer

	remoteSet       bool
	pendingICE      []webrtc.ICECandidateInit
	pendingICEBytes int
	signalMu        sync.Mutex
	signalSend      atomic.Uint64
	signalRecv      peer.ReplayWindow
	serverId        string
	roomId          string
	deadlineMu      sync.RWMutex
	writeDeadline   time.Time
	owner           *Server
	pendingLease    atomic.Bool
	totalLease      atomic.Bool
	handshakeMu     sync.Mutex
	handshakeTimer  *time.Timer
	closeOnce       sync.Once
	closeErr        error
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
	callbackPool      *peer.CallbackPool
	helloLimiter      *rate.Limiter
	totalConnections  atomic.Int64
	pendingHandshakes atomic.Int64
}
