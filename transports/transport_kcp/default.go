package transport_kcp

import "time"

const (
	// These values form a loss-tolerant Xray mKCP profile for ICE paths. Xray's
	// config defaults are conservative, while its own throughput tests use a
	// cwnd multiplier of 20.
	DefaultMTU              = 1200
	DefaultTTI              = 50
	DefaultUplinkCapacity   = 5
	DefaultDownlinkCapacity = 30
	DefaultCwndMultiplier   = 20
	DefaultMaxSendingWindow = 2 << 20
	DefaultDataShards       = 10
	DefaultParityShards     = 3
	closeMarkerFlushDelay   = 50 * time.Millisecond
	adaptiveCheckInterval   = 250 * time.Millisecond
	adaptiveFallbackRTO     = 750
	adaptiveFallbackAfter   = 750 * time.Millisecond
)

func DefaultTransport() Transport {
	return Transport{
		MTU:                       DefaultMTU,
		TTI:                       DefaultTTI,
		UplinkCapacity:            DefaultUplinkCapacity,
		DownlinkCapacity:          DefaultDownlinkCapacity,
		CwndMultiplier:            DefaultCwndMultiplier,
		MaxSendingWindow:          DefaultMaxSendingWindow,
		DataShards:                DefaultDataShards,
		ParityShards:              DefaultParityShards,
		AdaptiveCongestionControl: true,
		// The delivery-rate controller is the congestion controller in the
		// default profile. kcp-go's Reno window would otherwise become a second,
		// independent limiter.
		DisableCongestionControl: true,
	}
}
