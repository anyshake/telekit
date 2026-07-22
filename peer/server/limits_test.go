package server

import (
	"testing"
	"time"

	"github.com/alphadose/haxmap"
	"github.com/anyshake/telekit/peer"
)

func TestConnectionAndPendingHandshakeLimits(t *testing.T) {
	s := &Server{options: &Options{MaxConnections: 2, MaxPendingHandshakes: 1}}
	if !s.reserveConnection() {
		t.Fatal("first reservation rejected")
	}
	if s.reserveConnection() {
		t.Fatal("pending handshake limit was not enforced")
	}
	s.pendingHandshakes.Add(-1)
	s.totalConnections.Add(-1)
}

func TestHandshakeTimeoutReleasesConnection(t *testing.T) {
	s := &Server{
		options:     &Options{MaxConnections: 1, MaxPendingHandshakes: 1},
		connections: haxmap.New[string, *Connection](),
	}
	if !s.reserveConnection() {
		t.Fatal("reservation rejected")
	}
	conn := &Connection{sourceId: "client", owner: s, recvBuf: peer.NewRecvBuffer()}
	conn.pendingLease.Store(true)
	conn.totalLease.Store(true)
	s.connections.Set(conn.sourceId, conn)
	conn.startHandshakeTimer(5 * time.Millisecond)
	deadline := time.Now().Add(time.Second)
	for s.totalConnections.Load() != 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if s.totalConnections.Load() != 0 || s.pendingHandshakes.Load() != 0 {
		t.Fatalf("leases were not released: total=%d pending=%d", s.totalConnections.Load(), s.pendingHandshakes.Load())
	}
	if _, ok := s.connections.Get(conn.sourceId); ok {
		t.Fatal("expired handshake remains in connection map")
	}
}
