package transport_kcp

import (
	"sync"
	"time"
)

const (
	deliveryRateSampleInterval = 500 * time.Millisecond
	bbrWindowGain              = 2
)

// deliveryRateController is a BBR-like outer controller for kcp-go. kcp-go
// does not expose packet ACK/loss callbacks, so the delivery sample is taken
// from bytes accepted while Write is blocked by KCP's send window. That gives
// us the path's application delivery rate without pretending it is a full
// QUIC BBR implementation.
type deliveryRateController struct {
	mu          sync.Mutex
	sampleSince time.Time
	sampleBytes uint64
	maxRate     uint64
}

func (c *deliveryRateController) record(n int) {
	if n <= 0 {
		return
	}
	c.mu.Lock()
	if c.sampleSince.IsZero() {
		c.sampleSince = time.Now()
	}
	c.sampleBytes += uint64(n)
	c.mu.Unlock()
}

func (c *deliveryRateController) sample(now time.Time) uint64 {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.sampleSince.IsZero() || c.sampleBytes == 0 {
		return 0
	}
	elapsed := now.Sub(c.sampleSince)
	if elapsed < deliveryRateSampleInterval {
		return 0
	}

	rate := c.sampleBytes * uint64(time.Second) / uint64(elapsed)
	c.sampleBytes = 0
	c.sampleSince = now
	// Keep a short max filter like BBR's delivery-rate filter, but let it
	// decay so a stale burst cannot keep the window inflated forever.
	if rate > c.maxRate {
		c.maxRate = rate
	} else if c.maxRate > rate {
		c.maxRate = maxUint64(rate, c.maxRate*7/8)
	}
	return c.maxRate
}

func bbrTargetWindow(rateBytesPerSecond uint64, srttMillis int32, mtu, maxWindow int) int {
	if rateBytesPerSecond == 0 || srttMillis <= 0 || mtu <= 0 || maxWindow <= 0 {
		return 0
	}
	bdpBytes := rateBytesPerSecond * uint64(srttMillis) / 1000
	target := (bdpBytes*bbrWindowGain + uint64(mtu) - 1) / uint64(mtu)
	if target < minControlWindow {
		target = minControlWindow
	}
	if target > uint64(maxWindow) {
		target = uint64(maxWindow)
	}
	return int(target)
}

func maxUint64(a, b uint64) uint64 {
	if a > b {
		return a
	}
	return b
}
