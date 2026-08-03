package client_test

import (
	"bytes"
	"crypto/ed25519"
	"crypto/rand"
	"errors"
	"io"
	"testing"
	"time"

	"github.com/anyshake/telekit/peer"
	peerapi "github.com/anyshake/telekit/peer/api"
	peerclient "github.com/anyshake/telekit/peer/client"
	peerserver "github.com/anyshake/telekit/peer/server"
	"github.com/anyshake/telekit/transports"
	transportkcp "github.com/anyshake/telekit/transports/transport_kcp"
	transportrawudp "github.com/anyshake/telekit/transports/transport_rawudp"
	transportsctp "github.com/anyshake/telekit/transports/transport_sctp"
)

func TestTransportLifecycle(t *testing.T) {
	for _, transportName := range []string{"kcp", "sctp", "udp"} {
		t.Run(transportName, func(t *testing.T) {
			serverTransport := newTestTransport(transportName)
			clientTransport := newTestTransport(transportName)
			bus := newMemoryBus()
			serverAdapter := &memoryAdapter{bus: bus}
			clientAdapter := &memoryAdapter{bus: bus}
			key := bytes.Repeat([]byte{0x42}, peer.MinPreSharedKeySize)
			serverPublicKey, serverIdentityKey, err := ed25519.GenerateKey(rand.Reader)
			if err != nil {
				t.Fatal(err)
			}
			room := "lifecycle-" + transportName
			serverAPI, err := peerapi.NewAPI(room, serverAdapter)
			if err != nil {
				t.Fatal(err)
			}
			listener, err := peerserver.NewServer(serverAPI, &peerserver.Options{
				KeyProvider:      peer.StaticKeyring{"client-one": key},
				IdentityKey:      serverIdentityKey,
				Transports:       []transports.ITransport{serverTransport},
				HandshakeTimeout: 5 * time.Second,
			})
			if err != nil {
				t.Fatal(err)
			}
			if err := listener.Listen(); err != nil {
				t.Fatal(err)
			}
			defer listener.Close()

			clientAPI, err := peerapi.NewAPI(room, clientAdapter)
			if err != nil {
				t.Fatal(err)
			}
			conn, err := peerclient.NewClient(peer.PreSharedKey{
				ClientID: "client-one", Key: key, ServerPublicKey: serverPublicKey,
			}, clientAPI, &peerclient.Options{Timeout: 5 * time.Second, Transport: clientTransport})
			if err != nil {
				t.Fatal(err)
			}
			defer conn.Close()

			if err := conn.Connect(); err != nil {
				t.Fatal(err)
			}
			first, err := listener.Accept()
			if err != nil {
				t.Fatal(err)
			}
			if err := conn.Close(); err != nil {
				t.Fatal(err)
			}
			if err := first.SetReadDeadline(time.Now().Add(2 * time.Second)); err != nil {
				t.Fatal(err)
			}
			if _, err := first.Read(make([]byte, 1)); !errors.Is(err, io.EOF) {
				t.Fatalf("first server connection read error = %v", err)
			}

			if err := conn.Connect(); err != nil {
				t.Fatal(err)
			}
			second, err := listener.Accept()
			if err != nil {
				t.Fatal(err)
			}
			payloadSize := 2000
			if transportName == "udp" {
				// raw_udp preserves datagram boundaries and does not fragment.
				payloadSize = 512
			}
			payload := bytes.Repeat([]byte("proxy-data-"), payloadSize/len("proxy-data-"))
			if _, err := conn.Write(payload); err != nil {
				t.Fatal(err)
			}
			received := make([]byte, len(payload))
			if _, err := io.ReadFull(second, received); err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(received, payload) {
				t.Fatal("transport payload mismatch")
			}
			_ = second.Close()
		})
	}
}

func newTestTransport(name string) transports.ITransport {
	switch name {
	case "kcp":
		return transportkcp.New()
	case "sctp":
		return transportsctp.New()
	default:
		return transportrawudp.New()
	}
}
