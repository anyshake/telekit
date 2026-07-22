package websocket

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/anyshake/telekit/signaling"
)

func TestAdapterRoutesByRoomPathAndType(t *testing.T) {
	server := httptest.NewServer(NewBroker(WithAuthorization(func(*http.Request, string) bool { return true })))
	defer server.Close()
	endpoint := "ws" + strings.TrimPrefix(server.URL, "http")

	sender, err := NewAdapter(endpoint)
	if err != nil {
		t.Fatal(err)
	}
	receiver, err := NewAdapter(endpoint)
	if err != nil {
		t.Fatal(err)
	}
	if err := sender.Connect(); err != nil {
		t.Fatal(err)
	}
	defer sender.Disconnect()
	if err := receiver.Connect(); err != nil {
		t.Fatal(err)
	}
	defer receiver.Disconnect()

	received := make(chan []byte, 1)
	sub, err := receiver.Subscribe("room-one", signaling.MessageOffer, func(payload []byte) {
		received <- payload
	})
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Unsubscribe()
	if err := sender.Publish("room-one", signaling.MessageOffer, []byte("encrypted")); err != nil {
		t.Fatal(err)
	}
	select {
	case payload := <-received:
		if !bytes.Equal(payload, []byte("encrypted")) {
			t.Fatalf("payload = %q", payload)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for WebSocket signal")
	}
}
