package transport_kcp

import "testing"

func TestXrayKCPSettingsDefaults(t *testing.T) {
	settings := xrayKCPSettings(DefaultTransport())

	if settings.mtu != 1200 || settings.tti != 50 {
		t.Fatalf("unexpected MTU/TTI: got %d/%d", settings.mtu, settings.tti)
	}
	if settings.sendWindow != 1747 {
		t.Fatalf("unexpected send window: got %d, want 1747", settings.sendWindow)
	}
	if settings.receiveWindow != 1310 {
		t.Fatalf("unexpected receive window: got %d, want 1310", settings.receiveWindow)
	}
}

func TestXrayKCPSettingsFallbacks(t *testing.T) {
	settings := xrayKCPSettings(Transport{
		MTU:              1,
		TTI:              0,
		UplinkCapacity:   0,
		DownlinkCapacity: 0,
		CwndMultiplier:   0,
		MaxSendingWindow: 1,
	})

	if settings.mtu != DefaultMTU || settings.tti != DefaultTTI {
		t.Fatalf("invalid values were not replaced: got MTU/TTI %d/%d", settings.mtu, settings.tti)
	}
	if settings.sendWindow != 1 || settings.receiveWindow != 1310 {
		t.Fatalf("unexpected fallback windows: got %d/%d", settings.sendWindow, settings.receiveWindow)
	}
}

func TestXrayKCPSettingsMaxSendingWindow(t *testing.T) {
	settings := xrayKCPSettings(Transport{
		MTU:              DefaultMTU,
		TTI:              DefaultTTI,
		UplinkCapacity:   DefaultUplinkCapacity,
		DownlinkCapacity: DefaultDownlinkCapacity,
		CwndMultiplier:   20,
		MaxSendingWindow: 2 * 1024 * 1024,
	})

	if settings.sendWindow != 1747 {
		t.Fatalf("unexpected buffered send window: got %d, want 1747", settings.sendWindow)
	}
}

func TestBBRTargetWindow(t *testing.T) {
	if got := bbrTargetWindow(10*1024*1024, 50, 1200, 4096); got != 874 {
		t.Fatalf("unexpected BDP window: got %d, want 874", got)
	}
	if got := bbrTargetWindow(10*1024*1024, 50, 1200, 128); got != 128 {
		t.Fatalf("BDP window was not capped: got %d, want 128", got)
	}
	if got := bbrTargetWindow(0, 50, 1200, 4096); got != 0 {
		t.Fatalf("zero-rate target = %d, want 0", got)
	}
}
