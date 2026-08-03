package transport_sctp

const (
	DefaultMTU = 1200
	// A larger receive window matters on high-latency links: SCTP can keep
	// more data in flight while the application is draining net.Conn.
	DefaultMaxReceiveBuffer   = 32 << 20
	DefaultMaxMessageSize     = 60000
	DefaultEnableInterleaving = true
	// Let SCTP queue multiple application messages so the congestion window can
	// stay full across an RTT. The association still applies its own cwnd/rwnd
	// limits; callers that need strict producer backpressure can enable this.
	DefaultBlockWrite = false
	// These values tune Pion SCTP's native loss recovery and congestion
	// control for lossy/high-latency ICE paths. RTO max is in milliseconds;
	// the window values are bytes.
	DefaultRTOMax     = 30_000
	DefaultMinCwnd    = DefaultMTU * 8
	DefaultFastRtxWnd = DefaultMTU * 8
	DefaultCwndCAStep = DefaultMTU * 2
)

// DefaultTransport returns the SCTP/DataChannel settings used by telekit.
// The MTU leaves room for ICE/UDP overhead. Interleaving prevents one large
// ordered message from monopolizing the association, while non-blocking
// writes keep enough data queued to fill the cwnd on high-RTT paths.
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
