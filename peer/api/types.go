package api

import (
	"github.com/anyshake/telekit/signaling"
	"github.com/pion/webrtc/v4"
)

const DEFAULT_DATACHANNEL = "stream"

type Option func(*API) error

type API struct {
	RoomId          string
	DataChannel     string
	SignalingServer signaling.IAdapter
	WebRTCConfig    webrtc.Configuration
	WebRTCAPI       *webrtc.API
}
