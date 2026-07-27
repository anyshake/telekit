package transport_sctp

const (
	DefaultMTU = 1200
	// A larger receive window matters on high-latency links: SCTP can keep
	// more data in flight while the application is draining net.Conn.
	DefaultMaxReceiveBuffer   = 16 << 20
	DefaultMaxMessageSize     = 60000
	DefaultEnableInterleaving = true
	DefaultBlockWrite         = true
	// These values tune Pion SCTP's native loss recovery and congestion
	// control for lossy/high-latency ICE paths. RTO max is in milliseconds;
	// the window values are bytes.
	DefaultRTOMax     = 15_000
	DefaultMinCwnd    = DefaultMTU * 4
	DefaultFastRtxWnd = DefaultMTU * 4
	DefaultCwndCAStep = DefaultMTU * 2
)

// DefaultTransport returns the SCTP/DataChannel settings used by telekit.
// The MTU leaves room for ICE/UDP overhead, while interleaving and blocking
// writes avoid head-of-line stalls and partial association writes.
func DefaultTransport() Transport {
	return Transport{
		MTU:                DefaultMTU,
		MaxReceiveBuffer:   DefaultMaxReceiveBuffer,
		MaxMessageSize:     DefaultMaxMessageSize,
		EnableInterleaving: DefaultEnableInterleaving,
		BlockWrite:         DefaultBlockWrite,
		RTOMax:             DefaultRTOMax,
		MinCwnd:            DefaultMinCwnd,
		FastRtxWnd:         DefaultFastRtxWnd,
		CwndCAStep:         DefaultCwndCAStep,
	}
}
