package client

import (
	"net"
	"sync/atomic"
	"testing"

	"github.com/anyshake/telekit/peer"
	"github.com/pion/ice/v4"
)

type closeTrackingConn struct {
	net.Conn
	closed atomic.Bool
}

func (c *closeTrackingConn) Close() error {
	c.closed.Store(true)
	return nil
}

func TestManualDisconnectRejectsStaleTransportInstall(t *testing.T) {
	agent := &ice.Agent{}
	client := &Client{iceAgent: agent}
	client.manualDisconnect.Store(true)
	transport := &closeTrackingConn{}

	if client.setTransportConn(agent, transport, &peer.DataChannel{}) {
		t.Fatal("manual disconnect allowed stale transport installation")
	}
	if !transport.closed.Load() {
		t.Fatal("rejected transport was not closed")
	}
	if conn, dataChannel := client.transportState(); conn != nil || dataChannel != nil {
		t.Fatal("rejected transport changed client state")
	}
}
