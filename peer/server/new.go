package server

import (
	"bytes"
	"crypto/ed25519"
	"errors"
	"time"

	"github.com/alphadose/haxmap"
	"github.com/anyshake/telekit/peer"
	"github.com/anyshake/telekit/peer/api"
	"github.com/anyshake/telekit/transports"
	transportquic "github.com/anyshake/telekit/transports/transport_quic"
	"github.com/anyshake/telekit/utils/compression"
	"github.com/anyshake/telekit/utils/encryption"
	lru "github.com/hashicorp/golang-lru/v2"
	"golang.org/x/time/rate"
)

func NewServer(api *api.API, options *Options) (*Server, error) {
	if api.RoomId == "" {
		return nil, errors.New("room ID is not set")
	}
	if api.SignalingServer == nil {
		return nil, errors.New("signaling server adapter is not set")
	}
	if options == nil {
		return nil, errors.New("server options are nil")
	}
	optionsCopy := *options
	options = &optionsCopy
	if options.KeyProvider == nil {
		return nil, errors.New("pre-shared key provider is required")
	}
	if options.GetTimeFunc == nil {
		options.GetTimeFunc = time.Now
	}
	if len(options.IdentityKey) != ed25519.PrivateKeySize {
		return nil, errors.New("Ed25519 server identity key is required")
	}
	canonicalIdentityKey := ed25519.NewKeyFromSeed(options.IdentityKey[:ed25519.SeedSize])
	if !bytes.Equal(canonicalIdentityKey, options.IdentityKey) {
		return nil, errors.New("invalid Ed25519 server identity key")
	}
	options.IdentityKey = append(ed25519.PrivateKey(nil), options.IdentityKey...)
	if options.Transport != nil {
		if len(options.Transports) != 0 {
			return nil, errors.New("server transport and transports cannot both be set")
		}
		options.Transports = []transports.ITransport{options.Transport}
	} else if len(options.Transports) == 0 {
		options.Transports = []transports.ITransport{transportquic.New()}
	} else {
		options.Transports = append([]transports.ITransport(nil), options.Transports...)
	}
	seenTransports := make(map[string]struct{}, len(options.Transports))
	for _, transport := range options.Transports {
		if transport == nil || transport.Name() == "" {
			return nil, errors.New("unsupported server transport")
		}
		if _, exists := seenTransports[transport.Name()]; exists {
			return nil, errors.New("duplicate server transport")
		}
		seenTransports[transport.Name()] = struct{}{}
	}
	if options.LRUSize == 0 {
		options.LRUSize = DEFAULT_LRU_SIZE
	}
	if options.ReplayProtection == 0 {
		options.ReplayProtection = DEFAULT_REPLAY_PROTECTION
	}
	if options.ClockSkew == 0 {
		options.ClockSkew = 30 * time.Second
	}
	if options.MaxFrameSize == 0 {
		options.MaxFrameSize = DEFAULT_MAX_FRAME_SIZE
	}
	if options.ReceiveBufferSize == 0 {
		options.ReceiveBufferSize = peer.DefaultReceiveBufferSize
	}
	if options.MaxBufferedBytes == 0 {
		options.MaxBufferedBytes = DEFAULT_MAX_BUFFERED
	}
	if options.MaxPendingICE == 0 {
		options.MaxPendingICE = DEFAULT_MAX_PENDING_ICE
	}
	if options.MaxPendingICEBytes == 0 {
		options.MaxPendingICEBytes = DEFAULT_MAX_PENDING_ICE_BYTES
	}
	if options.MaxConnections == 0 {
		options.MaxConnections = DEFAULT_MAX_CONNECTIONS
	}
	if options.MaxPendingHandshakes == 0 {
		options.MaxPendingHandshakes = DEFAULT_MAX_HANDSHAKES
	}
	if options.HandshakeTimeout == 0 {
		options.HandshakeTimeout = DEFAULT_HANDSHAKE_TIMEOUT
	}
	if options.HelloRateLimit == 0 {
		options.HelloRateLimit = 100
	}
	if options.HelloRateBurst == 0 {
		options.HelloRateBurst = 200
	}
	if options.MaxFrameSize < 1024 || options.ReceiveBufferSize < options.MaxFrameSize ||
		options.MaxBufferedBytes < int64(options.ReceiveBufferSize) || options.MaxPendingICE < 1 ||
		options.MaxPendingICEBytes < 1 ||
		options.MaxConnections < 1 || options.MaxPendingHandshakes < 1 ||
		options.MaxPendingHandshakes > options.MaxConnections || options.HandshakeTimeout <= 0 ||
		options.HelloRateLimit <= 0 || options.HelloRateBurst < 1 {
		return nil, errors.New("invalid server resource limits")
	}
	if options.UseCompression && options.MaxFrameSize > compression.MaxDecodedSize {
		return nil, errors.New("compressed frame size exceeds decompression safety limit")
	}
	if options.EncryptionType == "" {
		options.EncryptionType = encryption.CHACHA20_POLY1305
	}
	if len(options.EncryptionAAD) == 0 {
		options.EncryptionAAD = []byte("telekit/v1")
	}

	nonceCache, err := lru.New[string, int64](options.LRUSize)
	if err != nil {
		return nil, err
	}

	serverPublicKey := options.IdentityKey.Public().(ed25519.PublicKey)
	serverID, err := peer.ServerIDFromPublicKey(serverPublicKey)
	if err != nil {
		return nil, err
	}
	server := &Server{
		serverId:     serverID,
		api:          api,
		options:      options,
		nonceCache:   nonceCache,
		defaultCodec: &peer.Codec{},
		connections:  haxmap.New[string, *Connection](),
		acceptCh:     make(chan *Connection, options.MaxConnections),
		closeCh:      make(chan struct{}),
		bufferBudget: peer.NewByteBudget(options.MaxBufferedBytes),
		helloLimiter: rate.NewLimiter(rate.Limit(options.HelloRateLimit), options.HelloRateBurst),
	}
	return server, nil
}
