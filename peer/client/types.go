package client

import (
	"bytes"
	"sync"
	"sync/atomic"
	"time"

	"github.com/anyshake/telekit/peer"
	"github.com/anyshake/telekit/peer/api"
	"github.com/pion/webrtc/v4"
)

const (
	DEFAULT_TIMEOUT               = 30 * time.Second
	MAX_CHUNK_SIZE                = 60000
	DEFAULT_MAX_FRAME_SIZE        = 4 << 20
	DEFAULT_MAX_PENDING_ICE       = 128
	DEFAULT_MAX_PENDING_ICE_BYTES = 256 << 10
	DEFAULT_CALLBACK_WORKERS      = 2
	DEFAULT_CALLBACK_QUEUE        = 64
	DEFAULT_MAX_SEND_BUFFER       = 1 << 20
	DEFAULT_MAX_CALLBACK_BYTES    = 16 << 20
)

type Options struct {
	// TimeFunc returns the current time
	// for generating timestamps
	// during handshaking
	TimeFunc func() time.Time
	// Timeout is the maximum time to wait
	// for a connection to be established.
	Timeout time.Duration
	// Compress data before sending
	UseCompression     bool
	MaxFrameSize       int
	ReceiveBufferSize  int
	MaxSendBufferSize  int
	MaxPendingICE      int
	MaxPendingICEBytes int
	CallbackWorkers    int
	CallbackQueueSize  int
	MaxCallbackBytes   int64
	// ReceiveEventsOnly delivers incoming application data only through
	// OnDataChannelMessage. It avoids retaining a duplicate stream copy when
	// the application does not use Read.
	ReceiveEventsOnly bool
	// DataChannelInit allows customizing the WebRTC DataChannel configuration,
	// such as Ordered, MaxPacketLifeTime, MaxRetransmits, etc., for unreliable transmission.
	DataChannelInit *webrtc.DataChannelInit
	// Additional data used when
	// encrypting
	EncryptionAAD []byte
	// Encryption type
	EncryptionType string
	// Callback when client hello
	// message is sent
	OnClientHello func(*Client)
	// Callback when server hello
	// message is received
	OnServerHello        func(*Client)
	OnOfferSent          func(*Client, *webrtc.SessionDescription)
	OnServerAnswer       func(*Client, *webrtc.SessionDescription)
	OnICECandidateSent   func(*Client, webrtc.ICECandidateInit)
	OnConnectionFailed   func(*Client, error)
	OnDisconnected       func(*Client)
	OnDataChannelOpen    func(*Client)
	OnDataChannelClose   func(*Client)
	OnDataChannelMessage func(*Client, []byte)
	OnDataChannelError   func(*Client, error)
}

type dataChunk struct {
	mu          sync.Mutex
	expectedLen uint64
	recvBuffer  bytes.Buffer
}

type Client struct {
	clientId string
	serverId string
	psk      peer.PreSharedKey
	api      *api.API
	options  *Options
	codec    *peer.Codec

	remoteSet       bool
	pendingICE      []webrtc.ICECandidateInit
	pendingICEBytes int
	signalMu        sync.Mutex
	signalSend      atomic.Uint64
	signalRecv      peer.ReplayWindow

	pc      *webrtc.PeerConnection
	dc      *webrtc.DataChannel
	stateMu sync.RWMutex
	bufferedAmountLowCh chan struct{}

	dataChunkBuf dataChunk
	writeMu      sync.Mutex

	// recvBuf is the TCP-like receive buffer. Use Read() to consume data.
	// Replaced with a fresh buffer on each ConnectWithContext call.
	recvBuf atomic.Pointer[peer.RecvBuffer]

	mu             sync.RWMutex
	connected      bool
	connecting     bool
	deadlineMu     sync.RWMutex
	writeDeadline  time.Time
	callbackMu     sync.Mutex
	callbackPool   *peer.CallbackPool
	callbackBudget *peer.ByteBudget
}
