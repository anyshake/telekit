package centrifugo

import (
	"testing"

	"github.com/anyshake/telekit/signaling"
)

func TestCustomBaseChannel(t *testing.T) {
	adapter, err := NewAdapter("ws://127.0.0.1:8000/connection/websocket", WithBaseChannel("sensors:telekit"))
	if err != nil {
		t.Fatal(err)
	}
	impl := adapter.(*AdapterImpl)
	if got, want := impl.topic("room1", signaling.MessageOffer), "sensors:telekit:room1:offer"; got != want {
		t.Fatalf("channel = %q, want %q", got, want)
	}
	if got, want := impl.SignalingID(), "centrifugo:ws://127.0.0.1:8000/connection/websocket:sensors:telekit"; got != want {
		t.Fatalf("SignalingID = %q, want %q", got, want)
	}
}

func TestDefaultBaseChannel(t *testing.T) {
	adapter, err := NewAdapter("ws://127.0.0.1:8000/connection/websocket")
	if err != nil {
		t.Fatal(err)
	}
	if got, want := adapter.(*AdapterImpl).topic("room1", signaling.MessageOffer), "telekit:room1:offer"; got != want {
		t.Fatalf("channel = %q, want %q", got, want)
	}
}

func TestInvalidBaseChannel(t *testing.T) {
	if _, err := NewAdapter("ws://127.0.0.1:8000/connection/websocket", WithBaseChannel("sensors::telekit")); err == nil {
		t.Fatal("empty base channel segment accepted")
	}
}
