package client

import "context"

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
