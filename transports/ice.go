package transports

import (
	"context"
	"errors"
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

// ValidateICEDescriptionLimits bounds the complete signaling representation
// before it is retained or passed to Pion ICE.
func ValidateICEDescriptionLimits(description ICEDescription, maxCandidates, maxBytes int) error {
	if maxCandidates < 1 || maxBytes < 1 {
		return errors.New("invalid ICE description limits")
	}
	if len(description.Candidates) > maxCandidates {
		return fmt.Errorf("ICE description has %d candidates, maximum is %d", len(description.Candidates), maxCandidates)
	}
	total := len(description.UsernameFragment) + len(description.Password)
	if total > maxBytes {
		return fmt.Errorf("ICE description exceeds %d bytes", maxBytes)
	}
	for _, candidate := range description.Candidates {
		if len(candidate) > maxBytes-total {
			return fmt.Errorf("ICE description exceeds %d bytes", maxBytes)
		}
		total += len(candidate)
	}
	return nil
}

func NewICEAgent(urls []*stun.URI, options ...ice.AgentOption) (*ice.Agent, error) {
	defaultOptions := []ice.AgentOption{
		ice.WithUrls(urls),
		ice.WithNetworkTypes([]ice.NetworkType{ice.NetworkTypeUDP4, ice.NetworkTypeUDP6}),
		ice.WithIncludeLoopback(),
		// ICE is used as a long-lived data path. Keep the selected NAT mapping alive.
		ice.WithKeepaliveInterval(2 * time.Second),
		// Keep alternate pairs checked so the controlling side can renominate
		// an IPv6 pair when the selected IPv4 path becomes unavailable.
		ice.WithRenomination(ice.DefaultNominationValueGenerator()),
		ice.WithAutomaticRenomination(3 * time.Second),
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

func ICEEndpoint(conn *ice.Conn, local, remote net.Addr, authKey []byte) Endpoint {
	normalized := normalizedConn{Conn: conn}
	return Endpoint{
		Conn:       normalized,
		PacketConn: NewPacketConn(normalized, local, remote),
		LocalAddr:  local,
		RemoteAddr: remote,
		AuthKey:    append([]byte(nil), authKey...),
	}
}
