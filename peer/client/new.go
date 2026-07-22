package client

import (
	"errors"
	"time"

	"github.com/anyshake/telekit/peer"
	"github.com/anyshake/telekit/peer/api"
	"github.com/anyshake/telekit/utils/compression"
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
		options.EncryptionType = encryption.XCHACHA20_POLY1305
	}
	if len(options.EncryptionAAD) == 0 {
		options.EncryptionAAD = []byte("telekit/v1")
	}
	if options.TimeFunc == nil {
		options.TimeFunc = time.Now
	}
	if options.Timeout == 0 {
		options.Timeout = DEFAULT_TIMEOUT
	}
	if options.MaxFrameSize == 0 {
		options.MaxFrameSize = DEFAULT_MAX_FRAME_SIZE
	}
	if options.ReceiveBufferSize == 0 {
		options.ReceiveBufferSize = peer.DefaultReceiveBufferSize
	}
	if options.MaxSendBufferSize == 0 {
		options.MaxSendBufferSize = DEFAULT_MAX_SEND_BUFFER
	}
	if options.MaxPendingICE == 0 {
		options.MaxPendingICE = DEFAULT_MAX_PENDING_ICE
	}
	if options.MaxPendingICEBytes == 0 {
		options.MaxPendingICEBytes = DEFAULT_MAX_PENDING_ICE_BYTES
	}
	if options.CallbackWorkers == 0 {
		options.CallbackWorkers = DEFAULT_CALLBACK_WORKERS
	}
	if options.CallbackQueueSize == 0 {
		options.CallbackQueueSize = DEFAULT_CALLBACK_QUEUE
	}
	if options.MaxCallbackBytes == 0 {
		options.MaxCallbackBytes = DEFAULT_MAX_CALLBACK_BYTES
	}
	if options.MaxFrameSize < 1024 || options.ReceiveBufferSize < options.MaxFrameSize ||
		options.MaxSendBufferSize < MAX_CHUNK_SIZE ||
		options.MaxPendingICE < 1 || options.MaxPendingICEBytes < 1 ||
		options.CallbackWorkers < 1 || options.CallbackQueueSize < 1 ||
		options.MaxCallbackBytes < int64(options.MaxFrameSize) {
		return nil, errors.New("invalid client resource limits")
	}
	if options.UseCompression && options.MaxFrameSize > compression.MaxDecodedSize {
		return nil, errors.New("compressed frame size exceeds decompression safety limit")
	}
	if options.ReceiveEventsOnly && options.OnDataChannelMessage == nil {
		return nil, errors.New("ReceiveEventsOnly requires OnDataChannelMessage")
	}

	codec, err := peer.NewCodec(options.EncryptionType, psk.Key, options.EncryptionAAD, options.UseCompression)
	if err != nil {
		return nil, err
	}

	c := &Client{
		clientId:       psk.ClientID,
		psk:            psk,
		api:            api,
		options:        options,
		codec:          codec,
		callbackBudget: peer.NewByteBudget(options.MaxCallbackBytes),
	}
	c.recvBuf.Store(peer.NewRecvBufferWithLimit(options.ReceiveBufferSize, nil))
	return c, nil
}
