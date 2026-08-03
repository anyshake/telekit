package relays

import (
	"context"
	"errors"
	"net"
	_ "unsafe"

	"github.com/pion/ice/v4"
)

//go:linkname pionAddCandidate github.com/pion/ice/v4.(*Agent).addCandidate
func pionAddCandidate(*ice.Agent, context.Context, ice.Candidate, net.PacketConn) error

func AddLocalCandidate(agent *ice.Agent, candidate ice.Candidate, candidateConn net.PacketConn) error {
	if agent == nil {
		return errors.New("ICE agent is nil")
	}
	if candidate == nil {
		return nil
	}
	if candidateConn == nil {
		return errors.New("candidate packet connection is nil")
	}

	return pionAddCandidate(agent, context.Background(), candidate, candidateConn)
}
