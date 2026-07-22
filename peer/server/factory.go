package server

import (
	"github.com/anyshake/telekit/peer"
	peerapi "github.com/anyshake/telekit/peer/api"
	"github.com/anyshake/telekit/signaling"
)

// NewListener builds and starts a room-scoped net.Listener. The application
// should run only one listener for a room; the signaling adapter is shared by
// all accepted clients.
func NewListener(roomID string, adapter signaling.Adapter, keys peer.KeyProvider, options *Options, apiOptions ...peerapi.Option) (*Server, error) {
	a, err := peerapi.NewAPI(roomID, adapter, apiOptions...)
	if err != nil {
		return nil, err
	}
	config := Options{}
	if options != nil {
		config = *options
	}
	config.KeyProvider = keys
	listener, err := NewServer(a, &config)
	if err != nil {
		return nil, err
	}
	if err := listener.Listen(); err != nil {
		_ = listener.Close()
		return nil, err
	}
	return listener, nil
}
