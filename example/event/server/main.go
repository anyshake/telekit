package main

import (
	"context"
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/hex"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/anyshake/telekit/peer"
	peerapi "github.com/anyshake/telekit/peer/api"
	"github.com/anyshake/telekit/peer/server"
	"github.com/anyshake/telekit/signaling"
	"github.com/anyshake/telekit/signaling/mqtt"
)

const demoServerIdentitySeed = "9d61b19deffd5a60ba844af492ec2cc44449c5697b326919703bac031cae7f60"

func main() {
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)
	log.SetPrefix("[telekit event/server] ")
	broker := flag.String("mqtt", "wss://mqtt-dashboard.com:8884/mqtt", "MQTT broker URL")
	mqttBaseTopic := flag.String("mqtt-base-topic", mqtt.DefaultBaseTopic, "MQTT base topic")
	room := flag.String("room", "event-demo", "room ID")
	secret := flag.String("secret", "change-me", "pre-shared passphrase accepted for demo clients")
	identitySeedHex := flag.String("identity-seed", demoServerIdentitySeed, "Ed25519 server identity seed (hex)")
	compression := flag.Bool("compression", false, "enable zstd compression before encryption")
	maxFrameBytes := flag.Int("max-frame-bytes", 4<<20, "maximum encrypted DataChannel frame size")
	receiveBufferBytes := flag.Int("receive-buffer-bytes", 8<<20, "maximum unread stream data per connection")
	sendBufferBytes := flag.Int("send-buffer-bytes", 1<<20, "DataChannel send-buffer backpressure threshold")
	maxBufferedBytes := flag.Int64("max-buffered-bytes", 256<<20, "server-wide reassembly, receive, ICE, and callback budget")
	maxPendingICE := flag.Int("max-pending-ice", 128, "maximum queued ICE Candidates per connection")
	maxPendingICEBytes := flag.Int("max-pending-ice-bytes", 256<<10, "maximum queued ICE Candidate bytes per connection")
	maxConnections := flag.Int("max-connections", 1024, "maximum total client connections")
	maxPendingHandshakes := flag.Int("max-pending-handshakes", 256, "maximum authenticated handshakes awaiting DataChannel open")
	handshakeTimeout := flag.Duration("handshake-timeout", 30*time.Second, "maximum lifetime of a pending handshake")
	helloRate := flag.Float64("hello-rate", 100, "ClientHello rate limit per second")
	helloBurst := flag.Int("hello-burst", 200, "ClientHello token-bucket burst")
	callbackWorkers := flag.Int("callback-workers", 4, "data callback worker count")
	callbackQueue := flag.Int("callback-queue", 128, "maximum queued data callbacks")
	mqttQueueMessages := flag.Int("mqtt-queue-messages", 1024, "maximum queued MQTT signaling messages")
	mqttQueueBytes := flag.Int("mqtt-queue-bytes", 16<<20, "maximum queued MQTT signaling bytes")
	flag.Parse()

	log.Printf("startup: room=%q signaling=mqtt base-topic=%q", *room, *mqttBaseTopic)
	log.Printf("limits: frame=%d receive=%d send=%d global=%d ice=%d/%d connections=%d pending=%d/%s hello=%.2f/%d callbacks=%d/%d mqtt=%d/%d",
		*maxFrameBytes, *receiveBufferBytes, *sendBufferBytes, *maxBufferedBytes,
		*maxPendingICE, *maxPendingICEBytes, *maxConnections, *maxPendingHandshakes, *handshakeTimeout,
		*helloRate, *helloBurst, *callbackWorkers, *callbackQueue, *mqttQueueMessages, *mqttQueueBytes)
	adapter, err := connectMQTT(*broker, *mqttBaseTopic, "event-server", *mqttQueueMessages, *mqttQueueBytes)
	if err != nil {
		log.Fatal(err)
	}
	defer adapter.Disconnect()
	a, err := peerapi.NewAPI(*room, adapter, peerapi.WithSTUNServer(
		"stun://stun.l.google.com:19302",
		"stun://stun1.l.google.com:19302",
		"stun://stun2.l.google.com:19302",
		"stun://stun3.l.google.com:19302",
		"stun://stun4.l.google.com:19302",
		"stun://global.stun.twilio.com:3478",
		"stun://jp1.stun.twilio.com:3478",
		"stun://sg1.stun.twilio.com:3478",
		"stun://us1.stun.twilio.com:3478",
		"stun://us2.stun.twilio.com:3478",
		"stun://stun.cloudflare.com:3478",
		"stun://stun.nextcloud.com:443",
		"stun://stun.nextcloud.com:3478",
		"stun://ipv4.am.i.mullvad.net:3478",
		"stun://ipv6.am.i.mullvad.net:3478",
		"stun://stun.fbsbx.com:3478",
		"stun://stun.sipgate.net:3478",
		"stun://relay.webwormhole.io:3478",
		"stun://stun.ipfire.org:3478",
		"stun://stun.sip.us:3478",
	))
	if err != nil {
		log.Fatal(err)
	}
	key := sha256.Sum256([]byte(*secret))
	identityKey, err := decodeIdentityKey(*identitySeedHex)
	if err != nil {
		log.Fatal(err)
	}
	s, err := server.NewServer(a, &server.Options{
		ReceiveEventsOnly:    true,
		IdentityKey:          identityKey,
		UseCompression:       *compression,
		MaxFrameSize:         *maxFrameBytes,
		ReceiveBufferSize:    *receiveBufferBytes,
		MaxSendBufferSize:    *sendBufferBytes,
		MaxBufferedBytes:     *maxBufferedBytes,
		MaxPendingICE:        *maxPendingICE,
		MaxPendingICEBytes:   *maxPendingICEBytes,
		MaxConnections:       *maxConnections,
		MaxPendingHandshakes: *maxPendingHandshakes,
		HandshakeTimeout:     *handshakeTimeout,
		HelloRateLimit:       *helloRate,
		HelloRateBurst:       *helloBurst,
		CallbackWorkers:      *callbackWorkers,
		CallbackQueueSize:    *callbackQueue,
		KeyProvider: peer.KeyProviderFunc(func(clientID string) ([]byte, error) {
			log.Printf("handshake: authenticating client=%q", clientID)
			return append([]byte(nil), key[:]...), nil
		}),
		OnNewClientReject: func(clientID string, err error) {
			log.Printf("handshake: rejected client=%q: %v", clientID, err)
		},
		OnDataChannelOpen: func(conn *server.Connection) {
			log.Printf("connection: opened client=%q local=%s remote=%s", conn.GetClientId(), conn.LocalAddr(), conn.RemoteAddr())
		},
		OnDataChannelMessage: func(conn *server.Connection, payload []byte) {
			log.Printf("data: received client=%q bytes=%d", conn.GetClientId(), len(payload))
			if _, err := conn.Write(payload); err != nil {
				log.Printf("data: echo failed client=%q: %v", conn.GetClientId(), err)
			}
		},
		OnDataChannelClose: func(conn *server.Connection) {
			log.Printf("connection: data channel closed client=%q", conn.GetClientId())
		},
		OnDisconnected: func(conn *server.Connection) {
			log.Printf("connection: disconnected client=%q", conn.GetClientId())
		},
		OnConnectionFailed: func(conn *server.Connection, err error) {
			log.Printf("connection: failed client=%q: %v", conn.GetClientId(), err)
		},
		OnDataChannelError: func(conn *server.Connection, err error) {
			log.Printf("connection: data channel error client=%q: %v", conn.GetClientId(), err)
		},
	})
	if err != nil {
		log.Fatal(err)
	}
	if err := s.Listen(); err != nil {
		log.Fatal(err)
	}
	defer s.Close()
	log.Printf("ready: listening addr=%s", s.Addr())

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	<-ctx.Done()
}

func decodeIdentityKey(value string) (ed25519.PrivateKey, error) {
	seed, err := hex.DecodeString(value)
	if err != nil {
		return nil, fmt.Errorf("decode identity seed: %w", err)
	}
	if len(seed) != ed25519.SeedSize {
		return nil, fmt.Errorf("identity seed must be %d bytes", ed25519.SeedSize)
	}
	return ed25519.NewKeyFromSeed(seed), nil
}

func connectMQTT(endpoint, baseTopic, role string, queueMessages, queueBytes int) (signaling.Adapter, error) {
	adapter, err := mqtt.NewMQTTAdapter(
		endpoint,
		mqtt.WithBaseTopic(baseTopic),
		mqtt.WithClientID(fmt.Sprintf("telekit-%s-%d", role, time.Now().UnixNano())),
		mqtt.WithConnectTimeout(10*time.Second),
		mqtt.WithKeepAlive(30*time.Second),
		mqtt.WithPingTimeout(10*time.Second),
		mqtt.WithDispatchQueueLimits(queueMessages, queueBytes),
		mqtt.WithDispatchOverflowHandler(func(topic string) { log.Printf("signaling: MQTT queue full; dropped topic=%q", topic) }),
		mqtt.WithConnectionLostHandler(func(err error) { log.Printf("signaling: MQTT disconnected: %v", err) }),
		mqtt.WithReconnectingHandler(func() { log.Println("signaling: MQTT reconnecting") }),
		mqtt.WithOnConnect(func() { log.Println("signaling: MQTT connected; subscriptions restored") }),
	)
	if err != nil {
		return nil, err
	}
	if err := adapter.Connect(); err != nil {
		return nil, err
	}
	return adapter, nil
}

func stunOptions() []peerapi.Option {
	return []peerapi.Option{peerapi.WithSTUNServer(
		"stun://stun.l.google.com:19302",
		"stun://stun1.l.google.com:19302",
		"stun://stun2.l.google.com:19302",
		"stun://stun3.l.google.com:19302",
		"stun://stun4.l.google.com:19302",
		"stun://global.stun.twilio.com:3478",
		"stun://jp1.stun.twilio.com:3478",
		"stun://sg1.stun.twilio.com:3478",
		"stun://us1.stun.twilio.com:3478",
		"stun://us2.stun.twilio.com:3478",
		"stun://stun.cloudflare.com:3478",
		"stun://stun.nextcloud.com:443",
		"stun://stun.nextcloud.com:3478",
		"stun://ipv4.am.i.mullvad.net:3478",
		"stun://ipv6.am.i.mullvad.net:3478",
		"stun://stun.fbsbx.com:3478",
		"stun://stun.sipgate.net:3478",
		"stun://relay.webwormhole.io:3478",
		"stun://stun.ipfire.org:3478",
		"stun://stun.sip.us:3478",
	)}
}
