package transport_raknet

import (
	"testing"
	"time"
)

func TestTransportDefaults(t *testing.T) {
	transport := New()
	if got, want := transport.Name(), "raknet"; got != want {
		t.Fatalf("Name() = %q, want %q", got, want)
	}

	pacing := transport.pacing()
	if pacing.minInterval != defaultMinWriteInterval {
		t.Fatalf("default minimum pacing = %s, want %s", pacing.minInterval, defaultMinWriteInterval)
	}
	if pacing.maxInterval != defaultMaxWriteInterval {
		t.Fatalf("default maximum pacing = %s, want %s", pacing.maxInterval, defaultMaxWriteInterval)
	}
	if pacing.window != defaultPacingWindow {
		t.Fatalf("default pacing window = %d, want %d", pacing.window, defaultPacingWindow)
	}
}

func TestWritePacingNormalization(t *testing.T) {
	pacing := (Transport{}).pacing()
	if pacing.minInterval <= 0 || pacing.maxInterval < pacing.minInterval || pacing.window <= 0 {
		t.Fatalf("invalid normalized default pacing: %+v", pacing)
	}

	pacing = (Transport{
		MinWriteInterval: 20 * time.Millisecond,
		MaxWriteInterval: 5 * time.Millisecond,
		PacingWindow:     -1,
	}).pacing()
	if pacing.minInterval != 20*time.Millisecond || pacing.maxInterval != 20*time.Millisecond || pacing.window != defaultPacingWindow {
		t.Fatalf("invalid normalized custom pacing: %+v", pacing)
	}
}
