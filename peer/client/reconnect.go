package client

import (
	"context"
	"time"
)

func (c *Client) Reconnect() error {
	ctx, cancel := context.WithTimeout(context.Background(), c.options.Timeout)
	defer cancel()
	return c.ReconnectWithContext(ctx)
}

func (c *Client) ReconnectWithContext(ctx context.Context) error {
	if err := c.Disconnect(); err != nil {
		return err
	}

	return c.ConnectWithContext(ctx)
}

// reconnectAfterTransportFailure performs a fresh signaling handshake and ICE
// negotiation after the application keepalive determines that the transport
// is no longer usable. The failed ICE agent is not reused.
func (c *Client) reconnectAfterTransportFailure(generation uint64) {
	for {
		if c.reconnectGeneration.Load() != generation || c.isConnected() {
			return
		}
		ctx, cancel := context.WithTimeout(context.Background(), c.options.Timeout)
		err := c.connectWithContext(ctx, true)
		cancel()
		if err == nil {
			if c.reconnectGeneration.Load() != generation {
				_ = c.disconnect(false, true)
			}
			return
		}
		if c.reconnectGeneration.Load() != generation {
			return
		}
		if c.options.OnConnectionFailed != nil {
			c.options.OnConnectionFailed(c, err)
		}
		timer := time.NewTimer(time.Second)
		<-timer.C
	}
}
