package transport_raknet

import "time"

const (
	// DefaultMTU keeps RakNet's MTU probes below the conservative packet size
	// used by the ICE transports. Larger probes can be fragmented or dropped
	// by NATs and VPNs before RakNet can establish its connection.
	DefaultMTU = 1200

	// RakNet exposes no send-window or unacknowledged-packet count through its
	// public API. These defaults keep bulk writes from outrunning a weak ICE
	// path while allowing the pacing to follow the measured RTT.
	defaultMinWriteInterval = 4 * time.Millisecond
	// Keep the adaptive limiter from turning a high-RTT connection into a
	// single-packet-at-a-time link. The retransmission queue is still bounded
	// by pacing, but the default should remain suitable for bulk transfers.
	defaultMaxWriteInterval = 25 * time.Millisecond
	defaultPacingWindow     = 16

	closeMarkerFlushDelay = 50 * time.Millisecond
)
