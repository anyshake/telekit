package server

import (
	"net"

	"github.com/anyshake/telekit/peer"
)

// Accept returns the next fully authenticated client after its negotiated
// transport is established. Connections from different clients are never bridged.
func (s *Server) Accept() (net.Conn, error) {
	select {
	case <-s.closeCh:
		return nil, net.ErrClosed
	default:
	}
	select {
	case conn := <-s.acceptCh:
		return conn, nil
	case <-s.closeCh:
		return nil, net.ErrClosed
	}
}

func (s *Server) Addr() net.Addr {
	return peer.Addr{RoomID: s.api.RoomId, PeerID: s.serverId}
}
