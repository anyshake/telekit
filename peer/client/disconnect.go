package client

import (
	"github.com/anyshake/telekit/peer"
	"github.com/anyshake/telekit/signaling"
)

func (c *Client) Disconnect() error {
	c.manualDisconnect.Store(true)
	return c.disconnect(true, true)
}

func (c *Client) disconnect(cancelReconnect, closeBuffer bool) error {
	if cancelReconnect {
		c.reconnectGeneration.Add(1)
	}
	if c.isConnected() {
		// KCP and SCTP do not reliably propagate a local socket close through
		// every ICE path. Tell the server which authenticated session to remove
		// before tearing down the data transport.
		_ = c.publishDisconnect()
	}
	c.setIsConnected(false)
	if closeBuffer {
		c.recvBuf.Load().Close()
	} else {
		c.recvBuf.Load().Reset()
	}

	c.stateMu.Lock()
	transportConn := c.transportConn
	c.transportConn = nil
	c.dataChannel = nil
	c.codec = nil
	c.serverId = ""
	agent := c.iceAgent
	c.iceAgent = nil
	c.localAddr = peer.Addr{}
	c.remoteAddr = peer.Addr{}
	c.stateMu.Unlock()

	var closeErr error
	if transportConn != nil {
		closeErr = transportConn.Close()
	}
	if agent != nil {
		if err := agent.Close(); closeErr == nil {
			closeErr = err
		}
	}

	return closeErr
}

func (c *Client) publishDisconnect() error {
	c.stateMu.RLock()
	serverID := c.serverId
	codec := c.codec
	c.stateMu.RUnlock()
	if serverID == "" || codec == nil {
		return nil
	}
	data, err := codec.EncodeMessage(&peer.Message{
		Header: &peer.Header{
			SourceId: c.clientId,
			TargetId: serverID,
			Type:     peer.MessageTypeDisconnect,
			Sequence: c.signalSend.Add(1),
		},
		Payload: &peer.Payload{},
	})
	if err != nil {
		return err
	}
	return c.api.SignalingServer.Publish(c.api.RoomId, signaling.MessageOffer, data)
}
