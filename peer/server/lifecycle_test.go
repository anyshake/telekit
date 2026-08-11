package server

import (
	"net"
	"sync/atomic"
	"testing"

	"github.com/alphadose/haxmap"
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

func TestStaleConnectionCannotDeleteReplacement(t *testing.T) {
	s := &Server{
		options:     &Options{},
		connections: haxmap.New[string, *Connection](),
		closeCh:     make(chan struct{}),
	}
	old := &Connection{sourceId: "client", owner: s, recvBuf: peer.NewRecvBuffer()}
	replacement := &Connection{sourceId: "client", owner: s, recvBuf: peer.NewRecvBuffer()}
	if !s.setConnection(old) || !s.setConnection(replacement) {
		t.Fatal("failed to install test connections")
	}

	if err := old.Close(); err != nil {
		t.Fatal(err)
	}
	current, ok := s.connections.Get("client")
	if !ok || current != replacement {
		t.Fatal("closing stale connection removed its replacement")
	}
}

func TestStaleConnectionRejectsAndClosesTransport(t *testing.T) {
	s := &Server{
		options:     &Options{},
		connections: haxmap.New[string, *Connection](),
		closeCh:     make(chan struct{}),
	}
	agent := &ice.Agent{}
	stale := &Connection{
		sourceId:    "client",
		owner:       s,
		iceAgent:    agent,
		dataChannel: &peer.DataChannel{},
	}
	replacement := &Connection{sourceId: "client", owner: s}
	if !s.setConnection(replacement) {
		t.Fatal("failed to install replacement connection")
	}
	transport := &closeTrackingConn{}

	if stale.installTransport(agent, transport, 1024, nil, nil) {
		t.Fatal("stale connection installed a transport")
	}
	if !transport.closed.Load() {
		t.Fatal("rejected transport was not closed")
	}
	if stale.markEstablished(agent, transport) {
		t.Fatal("stale connection was marked established")
	}
}

func TestClosedConnectionCannotBecomeEstablished(t *testing.T) {
	s := &Server{
		options:     &Options{},
		connections: haxmap.New[string, *Connection](),
		closeCh:     make(chan struct{}),
	}
	agent := &ice.Agent{}
	transport := &closeTrackingConn{}
	conn := &Connection{
		sourceId:      "client",
		owner:         s,
		iceAgent:      agent,
		transportConn: transport,
	}
	if !s.setConnection(conn) {
		t.Fatal("failed to install test connection")
	}
	conn.closed.Store(true)

	if conn.markEstablished(agent, transport) {
		t.Fatal("closed connection was marked established")
	}
	if conn.established.Load() {
		t.Fatal("closed connection has established state")
	}
}
