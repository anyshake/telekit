package client

import (
	"time"

	"github.com/anyshake/telekit/peer"
	peerapi "github.com/anyshake/telekit/peer/api"
	"github.com/anyshake/telekit/signaling"
)

// Dial is the compact client entry point: callers provide a room, timeout,
// signaling adapter, and their pre-shared identity. No server/peer ID is part
// of the public API.
func Dial(roomID string, timeout time.Duration, adapter signaling.Adapter, psk peer.PreSharedKey, apiOptions ...peerapi.Option) (*Client, error) {
	a, err := peerapi.NewAPI(roomID, adapter, apiOptions...)
	if err != nil {
		return nil, err
	}
	client, err := NewClient(psk, a, &Options{Timeout: timeout})
	if err != nil {
		return nil, err
	}
	if err := client.Connect(); err != nil {
		_ = client.Close()
		return nil, err
	}
	return client, nil
}
