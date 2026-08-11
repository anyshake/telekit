package client

import (
	"errors"
	"time"

	"github.com/anyshake/telekit/peer"
	"github.com/anyshake/telekit/peer/api"
	transportquic "github.com/anyshake/telekit/transports/transport_quic"
	"github.com/anyshake/telekit/utils/encryption"
)

func NewClient(psk peer.PreSharedKey, api *api.API, options *Options) (*Client, error) {
	if err := psk.Validate(); err != nil {
		return nil, err
	}
	psk.Key = append([]byte(nil), psk.Key...)
	psk.ServerPublicKey = append([]byte(nil), psk.ServerPublicKey...)
	if api == nil {
		return nil, errors.New("API is nil")
	}
	if options == nil {
		options = &Options{}
	} else {
		copy := *options
		options = &copy
	}

	if options.EncryptionType == "" {
		options.EncryptionType = encryption.CHACHA20_POLY1305
	}
	if len(options.EncryptionAAD) == 0 {
		options.EncryptionAAD = []byte("telekit/v1")
	}
	if options.GetTimeFunc == nil {
		options.GetTimeFunc = time.Now
	}
	if options.Timeout == 0 {
		options.Timeout = DEFAULT_TIMEOUT
	}
	if options.Transport == nil {
		options.Transport = transportquic.New()
	}
	if options.Transport.Name() == "" {
		return nil, errors.New("client transport name is empty")
	}
	if options.MaxFrameSize == 0 {
		options.MaxFrameSize = DEFAULT_MAX_FRAME_SIZE
	}
	if options.ReceiveBufferSize == 0 {
		options.ReceiveBufferSize = peer.DefaultReceiveBufferSize
	}
	if options.MaxPendingICE == 0 {
		options.MaxPendingICE = DEFAULT_MAX_PENDING_ICE
	}
	if options.MaxPendingICEBytes == 0 {
		options.MaxPendingICEBytes = DEFAULT_MAX_PENDING_ICE_BYTES
	}
	if options.MaxFrameSize < 1024 || options.ReceiveBufferSize < options.MaxFrameSize ||
		options.MaxPendingICE < 1 || options.MaxPendingICEBytes < 1 {
		return nil, errors.New("invalid client resource limits")
	}
	c := &Client{
		clientId: psk.ClientID,
		psk:      psk,
		api:      api,
		options:  options,
	}
	c.recvBuf.Store(peer.NewRecvBufferWithLimit(options.ReceiveBufferSize, nil))
	return c, nil
}
