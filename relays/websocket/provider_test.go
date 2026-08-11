package websocket

import "testing"

func TestProviderTokenIsOptional(t *testing.T) {
	provider, err := NewProvider(ProviderConfig{
		URL:     "wss://relay.example.org/relay/server",
		Session: "session",
		LocalID: "client",
		PeerID:  "server",
	})
	if err != nil {
		t.Fatalf("empty token rejected: %v", err)
	}
	if provider.config.Token != "" {
		t.Fatal("provider changed the empty token")
	}
}
