package transports

import (
	"context"
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/pion/ice/v4"
	"github.com/pion/stun/v3"
)

type ICEDescription struct {
	UsernameFragment string
	Password         string
	Candidates       []string
}

func NewICEAgent(urls []*stun.URI, options ...ice.AgentOption) (*ice.Agent, error) {
	defaultOptions := []ice.AgentOption{
		ice.WithUrls(urls),
		ice.WithNetworkTypes([]ice.NetworkType{ice.NetworkTypeUDP4, ice.NetworkTypeUDP6}),
		ice.WithIncludeLoopback(),
		// ICE is used as a long-lived data path. Keep the selected NAT
		// mapping alive, but do not declare a temporarily congested path dead
		// as quickly as the Pion defaults do.
		ice.WithKeepaliveInterval(2 * time.Second),
		ice.WithDisconnectedTimeout(15 * time.Second),
		ice.WithFailedTimeout(60 * time.Second),
		// A shorter check interval reduces setup latency on high RTT paths.
		// More binding attempts make transient loss during hole punching less
		// likely to force a full reconnect.
		ice.WithCheckInterval(100 * time.Millisecond),
		ice.WithMaxBindingRequests(12),
		ice.WithSrflxAcceptanceMinWait(150 * time.Millisecond),
	}
	return ice.NewAgentWithOptions(append(defaultOptions, options...)...)
}

func GatherICEWithCallback(agent *ice.Agent, onCandidate func(ice.Candidate), beforeGather func() error) (ICEDescription, error) {
	if agent == nil {
		return ICEDescription{}, fmt.Errorf("ICE agent is nil")
	}
	// Do not send candidates through a bounded channel while GatherCandidates
	// is still running. A server with several interfaces/STUN responses can
	// produce more candidates than the old 32-entry buffer, which could block
	// Pion's callback and make gathering appear to hang.
	var candidatesMu sync.Mutex
	var candidates []string
	gathered := make(chan struct{})
	if err := agent.OnCandidate(func(candidate ice.Candidate) {
		if onCandidate != nil {
			onCandidate(candidate)
		}
		if candidate == nil {
			close(gathered)
			return
		}
		candidatesMu.Lock()
		candidates = append(candidates, candidate.Marshal())
		candidatesMu.Unlock()
	}); err != nil {
		return ICEDescription{}, err
	}
	if beforeGather != nil {
		if err := beforeGather(); err != nil {
			return ICEDescription{}, err
		}
	}
	if err := agent.GatherCandidates(); err != nil {
		return ICEDescription{}, err
	}

	// Pion dispatches candidate callbacks asynchronously. GatherCandidates
	// waits for candidate gathering, but not for the callback notifier, so wait
	// for its terminal nil callback before returning the description.
	<-gathered
	candidatesMu.Lock()
	candidatesCopy := append([]string(nil), candidates...)
	candidatesMu.Unlock()
	ufrag, password, err := agent.GetLocalUserCredentials()
	if err != nil {
		return ICEDescription{}, err
	}
	return ICEDescription{UsernameFragment: ufrag, Password: password, Candidates: candidatesCopy}, nil
}

func AddRemoteCandidates(agent *ice.Agent, description ICEDescription) error {
	if agent == nil {
		return fmt.Errorf("ICE agent is nil")
	}
	if err := agent.SetRemoteCredentials(description.UsernameFragment, description.Password); err != nil {
		return err
	}
	for _, raw := range description.Candidates {
		candidate, err := ice.UnmarshalCandidate(raw)
		if err != nil {
			return fmt.Errorf("parse ICE candidate: %w", err)
		}
		if err := agent.AddRemoteCandidate(candidate); err != nil {
			return err
		}
	}
	return nil
}

func DialICE(ctx context.Context, agent *ice.Agent, remote ICEDescription) (*ice.Conn, error) {
	if err := AddRemoteCandidates(agent, remote); err != nil {
		return nil, err
	}
	return agent.Dial(ctx, remote.UsernameFragment, remote.Password)
}

func AcceptICE(ctx context.Context, agent *ice.Agent, remote ICEDescription) (*ice.Conn, error) {
	if err := AddRemoteCandidates(agent, remote); err != nil {
		return nil, err
	}
	return agent.Accept(ctx, remote.UsernameFragment, remote.Password)
}

func ICEEndpoint(conn *ice.Conn, local, remote net.Addr) Endpoint {
	normalized := normalizedConn{Conn: conn}
	return Endpoint{
		Conn:       normalized,
		PacketConn: NewPacketConn(normalized, local, remote),
		LocalAddr:  local,
		RemoteAddr: remote,
	}
}
