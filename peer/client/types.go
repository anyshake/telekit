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
	// ICEAgentOptions are passed to the Pion ICE agent created for each connection.
	ICEAgentOptions []ice.AgentOption
	// GetTimeFunc returns the current time used by handshake, replay, keepalive,
	// and deadline checks. If nil, time.Now is used.
	GetTimeFunc func() time.Time
	// Timeout is the maximum time to wait for a connection to be established.
	Timeout time.Duration
	// Transport selects the data transport negotiated after ICE. Nil defaults to
	// the reliable QUIC transport.
	Transport transports.ITransport
	// MaxFrameSize is the maximum decoded encrypted transport frame size.
	MaxFrameSize int
	// ReceiveBufferSize is the maximum unread application data retained by Read.
	ReceiveBufferSize int
	// MaxPendingICE limits the number of candidates in one ICE description.
	MaxPendingICE int
	// MaxPendingICEBytes limits credentials and candidate bytes in one ICE description.
	MaxPendingICEBytes int
	// EncryptionAAD is additional authenticated data included in each frame.
	EncryptionAAD []byte
	// EncryptionType selects the frame cipher. Empty uses ChaCha20-Poly1305.
	EncryptionType string
	// OnClientHello is called after ClientHello is sent.
	OnClientHello func(*Client)
	// OnServerHello is called after a valid ServerHello is received.
	OnServerHello func(*Client)
	// OnICECandidate is called for each locally gathered ICE candidate.
	OnICECandidate func(*Client, ice.Candidate)
	// OnICECandidateGatheringComplete is called after local ICE candidate
	// gathering has completed.
	OnICECandidateGatheringComplete func(*Client)
	// OnICEOffer is called before the local ICE offer is published.
	OnICEOffer func(*Client, transports.ICEDescription)
	// OnICEAnswer is called after a valid ICE answer is received.
	OnICEAnswer func(*Client, transports.ICEDescription)
	// OnConnected is called after ICE and the selected transport are connected.
	OnConnected func(*Client)
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
	codec    *peer.SignalingChannel

	signalMu   sync.Mutex
	signalSend atomic.Uint64
	signalRecv peer.ReplayWindow
	// Kept only for decoding compatibility with pre-ICE callers. New handshakes
	// exchange a complete ICE description and never use this queue.
	pendingICE      []webrtc.ICECandidateInit
	pendingICEBytes int

	transportConn       net.Conn
	dataChannel         *peer.DataChannel
	lastTransportRead   atomic.Int64
	reconnectGeneration atomic.Uint64
	manualDisconnect    atomic.Bool
	iceAgent            *ice.Agent
	localAddr           peer.Addr
	remoteAddr          peer.Addr
	stateMu             sync.RWMutex

	writeMu sync.Mutex

	// recvBuf is the TCP-like receive buffer. Use Read() to consume data.
	// Reused across automatic transport reconnects and replaced after an
	// explicit Disconnect.
	recvBuf atomic.Pointer[peer.RecvBuffer]

	mu            sync.RWMutex
	connected     bool
	connecting    bool
	deadlineMu    sync.RWMutex
	writeDeadline time.Time
}
