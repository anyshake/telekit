package server

import (
	"github.com/anyshake/telekit/peer"
	"github.com/pion/webrtc/v4"
)

func (c *Connection) GetClientId() string {
	return c.sourceId
}

func (c *Connection) GetCodec() *peer.Codec {
	return c.codec
}

func (c *Connection) GetPeerConnection() *webrtc.PeerConnection {
	return c.peerConnection()
}

func (c *Connection) GetDataChannel() *webrtc.DataChannel {
	return c.dataChannel()
}
