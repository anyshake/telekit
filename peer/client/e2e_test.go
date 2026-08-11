package client_test

import (
	"bytes"
	"crypto/ed25519"
	"crypto/rand"
	"errors"
	"io"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/anyshake/telekit/peer"
	peerapi "github.com/anyshake/telekit/peer/api"
	"github.com/anyshake/telekit/peer/client"
	peerserver "github.com/anyshake/telekit/peer/server"
	"github.com/anyshake/telekit/signaling"
	transportkcp "github.com/anyshake/telekit/transports/transport_kcp"
)

type memoryBus struct {
	mu       sync.RWMutex
	nextID   atomic.Uint64
	handlers map[string]map[uint64]signaling.Handler
}

type memoryAdapter struct{ bus *memoryBus }

func newMemoryBus() *memoryBus {
	return &memoryBus{handlers: make(map[string]map[uint64]signaling.Handler)}
}
func (a *memoryAdapter) Connect() error    { return nil }
func (a *memoryAdapter) Disconnect() error { return nil }

func route(room string, typ signaling.MessageType) string { return room + "/" + string(typ) }

func (a *memoryAdapter) Publish(room string, typ signaling.MessageType, payload []byte) error {
	a.bus.mu.RLock()
	handlers := make([]signaling.Handler, 0, len(a.bus.handlers[route(room, typ)]))
	for _, handler := range a.bus.handlers[route(room, typ)] {
		handlers = append(handlers, handler)
	}
	a.bus.mu.RUnlock()
	for _, handler := range handlers {
		data := append([]byte(nil), payload...)
		go handler(data)
	}
	return nil
}

func (a *memoryAdapter) Subscribe(room string, typ signaling.MessageType, handler signaling.Handler) (signaling.Subscription, error) {
	id := a.bus.nextID.Add(1)
	key := route(room, typ)
	a.bus.mu.Lock()
	if a.bus.handlers[key] == nil {
		a.bus.handlers[key] = make(map[uint64]signaling.Handler)
	}
	a.bus.handlers[key][id] = handler
	a.bus.mu.Unlock()
	return signaling.NewSubscription(func() error {
		a.bus.mu.Lock()
		delete(a.bus.handlers[key], id)
		a.bus.mu.Unlock()
		return nil
	}), nil
}

func TestEncryptedNetConnRoundTrip(t *testing.T) {
	bus := newMemoryBus()
	serverAdapter := &memoryAdapter{bus: bus}
	clientAdapter := &memoryAdapter{bus: bus}
	key := bytes.Repeat([]byte{0x42}, peer.MinPreSharedKeySize)
	serverPublicKey, serverIdentityKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}

	serverAPI, err := peerapi.NewAPI("test-room", serverAdapter)
	if err != nil {
		t.Fatal(err)
	}
	listener, err := peerserver.NewServer(serverAPI, &peerserver.Options{
		KeyProvider: peer.StaticKeyring{"client-one": key},
		IdentityKey: serverIdentityKey,
		Transport:   transportkcp.DefaultTransport(),
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := listener.Listen(); err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	second, err := peerserver.NewServer(serverAPI, &peerserver.Options{
		KeyProvider: peer.StaticKeyring{"client-one": key},
		IdentityKey: serverIdentityKey,
		Transport:   transportkcp.DefaultTransport(),
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := second.Listen(); err == nil {
		_ = second.Close()
		t.Fatal("second server was allowed to listen in the same room")
	}

	clientAPI, err := peerapi.NewAPI("test-room", clientAdapter)
	if err != nil {
		t.Fatal(err)
	}
	conn, err := client.NewClient(peer.PreSharedKey{ClientID: "client-one", Key: key, ServerPublicKey: serverPublicKey}, clientAPI, &client.Options{
		Timeout:   10 * time.Second,
		Transport: transportkcp.DefaultTransport(),
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := conn.Connect(); err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	serverConn, err := listener.Accept()
	if err != nil {
		t.Fatal(err)
	}
	defer serverConn.Close()
	if conn.LocalAddr().Network() != "telekit" || serverConn.RemoteAddr().String() != conn.LocalAddr().String() {
		t.Fatalf("unexpected addresses: client=%s server remote=%s", conn.LocalAddr(), serverConn.RemoteAddr())
	}

	payload := make([]byte, 240000)
	if _, err := rand.Read(payload); err != nil {
		t.Fatal(err)
	}
	if n, err := conn.Write(payload); err != nil || n != len(payload) {
		t.Fatalf("client Write = %d, %v", n, err)
	}
	received := make([]byte, len(payload))
	if _, err := io.ReadFull(serverConn, received); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(received, payload) {
		t.Fatal("client to server payload mismatch")
	}

	reply := []byte("server-only")
	if n, err := serverConn.Write(reply); err != nil || n != len(reply) {
		t.Fatalf("server Write = %d, %v", n, err)
	}
	gotReply := make([]byte, len(reply))
	if _, err := io.ReadFull(conn, gotReply); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(gotReply, reply) {
		t.Fatal("server to client payload mismatch")
	}

	if err := conn.SetReadDeadline(time.Now().Add(20 * time.Millisecond)); err != nil {
		t.Fatal(err)
	}
	if _, err := conn.Read(make([]byte, 1)); !errors.Is(err, os.ErrDeadlineExceeded) {
		t.Fatalf("deadline Read error = %v", err)
	}
}
