package nats

import (
	"testing"

	"github.com/anyshake/telekit/signaling"
	natsgo "github.com/nats-io/nats.go"
)

func TestCustomBaseSubject(t *testing.T) {
	adapter, err := NewAdapterWithBaseSubject(
		"nats://127.0.0.1:4222",
		"sensors.telekit",
		natsgo.Name("routing-test"),
	)
	if err != nil {
		t.Fatal(err)
	}
	impl := adapter.(*Adapter)
	if got, want := impl.subject("room1", signaling.MessageOffer), "sensors.telekit.room1.offer"; got != want {
		t.Fatalf("subject = %q, want %q", got, want)
	}
	if got, want := impl.SignalingID(), "nats:nats://127.0.0.1:4222:sensors.telekit"; got != want {
		t.Fatalf("SignalingID = %q, want %q", got, want)
	}
}

func TestDefaultBaseSubject(t *testing.T) {
	adapter, err := NewAdapter("nats://127.0.0.1:4222", natsgo.Name("default-routing-test"))
	if err != nil {
		t.Fatal(err)
	}
	if got, want := adapter.(*Adapter).subject("room1", signaling.MessageOffer), "telekit.room1.offer"; got != want {
		t.Fatalf("subject = %q, want %q", got, want)
	}
}

func TestInvalidBaseSubject(t *testing.T) {
	if _, err := NewAdapterWithBaseSubject("nats://127.0.0.1:4222", "sensors.>.telekit"); err == nil {
		t.Fatal("wildcard base subject accepted")
	}
}
