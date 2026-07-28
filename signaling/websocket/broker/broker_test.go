package broker

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestBrokerDeniesRequestsWithoutAuthorizationPolicy(t *testing.T) {
	recorder := httptest.NewRecorder()
	NewBroker().ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/room", nil))
	if recorder.Code != http.StatusForbidden {
		t.Fatalf("status = %d, want %d", recorder.Code, http.StatusForbidden)
	}
}

func TestBrokerAuthorizationReceivesRoom(t *testing.T) {
	var room string
	recorder := httptest.NewRecorder()
	broker := NewBroker(WithAuthorization(func(_ *http.Request, roomID string) bool {
		room = roomID
		return false
	}))
	broker.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/sensor-room", nil))
	if room != "sensor-room" || recorder.Code != http.StatusForbidden {
		t.Fatalf("room/status = %q/%d", room, recorder.Code)
	}
}

func TestBrokerClientQueueByteLimit(t *testing.T) {
	client := &brokerClient{
		send:          make(chan outboundMessage, 2),
		done:          make(chan struct{}),
		maxQueueBytes: 3,
	}
	if !client.enqueue(outboundMessage{data: []byte("12")}) {
		t.Fatal("first enqueue rejected")
	}
	if client.enqueue(outboundMessage{data: []byte("34")}) {
		t.Fatal("enqueue above byte limit accepted")
	}
}
