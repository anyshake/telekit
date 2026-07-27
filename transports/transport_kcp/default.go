package transport_kcp

import "time"

const (
	// ICE paths share the same conservative packet budget as the QUIC and
	// SCTP transports. kcp-go defaults to 1400, which can fragment or be
	// dropped before congestion control has a chance to react.
	DefaultMTU            = 1200
	DefaultDataShards     = 10
	DefaultParityShards   = 3
	closeMarkerFlushDelay = 50 * time.Millisecond
	adaptiveCheckInterval = 250 * time.Millisecond
	adaptiveFallbackRTO   = 750
	adaptiveFallbackAfter = 750 * time.Millisecond
	adaptiveRecoveryAfter = 10 * time.Second
	adaptiveRecoveryRTO   = 400
)

func DefaultTransport() Transport {
	return Transport{
		MTU:                       DefaultMTU,
		DataShards:                DefaultDataShards,
		ParityShards:              DefaultParityShards,
		AdaptiveCongestionControl: true,
	}
}
