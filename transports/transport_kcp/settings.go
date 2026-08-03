package transport_kcp

const (
	minKCPMTU        = 50
	maxKCPMTU        = 1500
	minKCPInterval   = 10
	maxKCPInterval   = 1000
	minControlWindow = 16
)

type kcpSettings struct {
	mtu               int
	tti               int
	sendWindow        int
	initialSendWindow int
	receiveWindow     int
}

// xrayKCPSettings applies Xray mKCP's capacity-to-window model to kcp-go.
// kcp-go exposes a single send window instead of Xray's separate send buffer
// and control window, so the configured send window is bounded by both.
func xrayKCPSettings(t Transport) kcpSettings {
	mtu := t.MTU
	if mtu < minKCPMTU || mtu > maxKCPMTU {
		mtu = DefaultMTU
	}

	tti := t.TTI
	if tti < minKCPInterval || tti > maxKCPInterval {
		tti = DefaultTTI
	}

	uplinkCapacity := t.UplinkCapacity
	if uplinkCapacity == 0 {
		uplinkCapacity = DefaultUplinkCapacity
	}
	downlinkCapacity := t.DownlinkCapacity
	if downlinkCapacity == 0 {
		downlinkCapacity = DefaultDownlinkCapacity
	}
	cwndMultiplier := t.CwndMultiplier
	if cwndMultiplier == 0 {
		cwndMultiplier = DefaultCwndMultiplier
	}
	maxSendingWindow := t.MaxSendingWindow
	if maxSendingWindow <= 0 {
		maxSendingWindow = DefaultMaxSendingWindow
	}

	sendInFlight := xrayInFlightWindow(uplinkCapacity, mtu, tti)
	receiveInFlight := xrayInFlightWindow(downlinkCapacity, mtu, tti)
	sendBuffer := uint64(maxSendingWindow / mtu)
	if sendBuffer == 0 {
		sendBuffer = 1
	}

	sendWindow := uint64(sendInFlight) * uint64(cwndMultiplier)
	if sendWindow > sendBuffer {
		sendWindow = sendBuffer
	}
	sendWindow = uint64(clampKCPWindow(sendWindow))
	initialSendWindow := sendWindow
	if initialSendWindow > 128 {
		initialSendWindow = 128
	}

	return kcpSettings{
		mtu:               mtu,
		tti:               tti,
		sendWindow:        int(sendWindow),
		initialSendWindow: clampKCPWindow(initialSendWindow),
		receiveWindow:     clampKCPWindow(uint64(receiveInFlight)),
	}
}

// xrayInFlightWindow is equivalent to Config.Get*InFlightSize in Xray. The
// arithmetic is widened before multiplication so a large uint32 capacity
// cannot wrap before the minimum-window check.
func xrayInFlightWindow(capacity uint32, mtu, tti int) int {
	intervalsPerSecond := 1000 / tti
	size := uint64(capacity) * 1024 * 1024
	size /= uint64(mtu)
	size /= uint64(intervalsPerSecond)
	if size < 8 {
		size = 8
	}
	return clampKCPWindow(size)
}

func clampKCPWindow(size uint64) int {
	maxWindow := uint64(^uint32(0))
	if maxInt := uint64(^uint(0) >> 1); maxWindow > maxInt {
		maxWindow = maxInt
	}
	if size == 0 {
		return 1
	}
	if size > maxWindow {
		return int(maxWindow)
	}
	return int(size)
}
