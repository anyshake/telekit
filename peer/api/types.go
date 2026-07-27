package api

import (
	"github.com/anyshake/telekit/signaling"
	"github.com/pion/stun/v3"
)

type Option func(*API) error

type API struct {
	RoomId          string
	SignalingServer signaling.IAdapter
	// ICEURLs is used by the standalone Pion ICE agent after signaling.
	ICEURLs []*stun.URI
}
