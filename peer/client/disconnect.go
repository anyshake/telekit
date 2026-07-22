package client

import (
	"bytes"

	"github.com/anyshake/telekit/peer"
)

func (c *Client) Disconnect() error {
	c.setIsConnected(false)
	c.recvBuf.Load().Close()
	c.stopCallbackPool()
	c.dataChunkBuf.mu.Lock()
	c.dataChunkBuf.expectedLen = 0
	c.dataChunkBuf.recvBuffer = bytes.Buffer{}
	c.dataChunkBuf.mu.Unlock()
	c.signalMu.Lock()
	c.pendingICE = nil
	c.pendingICEBytes = 0
	c.signalMu.Unlock()

	pc, dc := c.takePeerConnection()
	var closeErr error
	if dc != nil {
		closeErr = dc.Close()
	}
	if pc != nil {
		if err := pc.Close(); closeErr == nil {
			closeErr = err
		}
	}

	return closeErr
}

func (c *Client) startCallbackPool() {
	c.callbackMu.Lock()
	if c.callbackPool != nil {
		c.callbackPool.Close()
	}
	if c.options.OnDataChannelMessage != nil {
		c.callbackPool = peer.NewCallbackPool(c.options.CallbackWorkers, c.options.CallbackQueueSize)
	} else {
		c.callbackPool = nil
	}
	c.callbackMu.Unlock()
}

func (c *Client) stopCallbackPool() {
	c.callbackMu.Lock()
	pool := c.callbackPool
	c.callbackPool = nil
	c.callbackMu.Unlock()
	pool.Close()
}

func (c *Client) submitDataCallback(data []byte) bool {
	c.callbackMu.Lock()
	pool := c.callbackPool
	if pool == nil || !c.callbackBudget.Reserve(len(data)) {
		c.callbackMu.Unlock()
		return false
	}
	release := func() { c.callbackBudget.Release(len(data)) }
	ok := pool.SubmitWithCancel(func() {
		defer release()
		c.options.OnDataChannelMessage(c, data)
	}, release)
	if !ok {
		release()
	}
	c.callbackMu.Unlock()
	return ok
}
