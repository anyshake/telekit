package mqtt

import (
	"testing"

	"github.com/anyshake/telekit/signaling"
)

func TestConfigurableBaseTopic(t *testing.T) {
	adapter, err := NewMQTTAdapter("tcp://127.0.0.1:1883", WithBaseTopic("sensors/telekit"))
	if err != nil {
		t.Fatal(err)
	}
	impl := adapter.(*MqttAdapterImpl)
	if got := impl.topic("room", signaling.MessageOffer); got != "sensors/telekit/room/offer" {
		t.Fatalf("topic = %q", got)
	}
	if impl.SignalingID() != "mqtt:tcp://127.0.0.1:1883:sensors/telekit" {
		t.Fatalf("signaling ID = %q", impl.SignalingID())
	}
}

func TestDefaultBaseTopic(t *testing.T) {
	adapter, err := NewMQTTAdapter("tcp://127.0.0.1:1883")
	if err != nil {
		t.Fatal(err)
	}
	if got, want := adapter.(*MqttAdapterImpl).topic("room", signaling.MessageOffer), "telekit/room/offer"; got != want {
		t.Fatalf("topic = %q, want %q", got, want)
	}
}

func TestBaseTopicRejectsWildcards(t *testing.T) {
	if _, err := NewMQTTAdapter("tcp://127.0.0.1:1883", WithBaseTopic("sensors/+/telekit")); err == nil {
		t.Fatal("MQTT wildcard base topic accepted")
	}
}
