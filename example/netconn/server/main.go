package main

import (
	"context"
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
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
	log.SetPrefix("[telekit netconn/server] ")
	broker := flag.String("mqtt", "wss://mqtt-dashboard.com:8884/mqtt", "MQTT broker URL")
	mqttBaseTopic := flag.String("mqtt-base-topic", mqtt.DefaultBaseTopic, "MQTT base topic")
	room := flag.String("room", "netconn-demo", "room ID")
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
	mqttQueueMessages := flag.Int("mqtt-queue-messages", 1024, "maximum queued MQTT signaling messages")
	mqttQueueBytes := flag.Int("mqtt-queue-bytes", 16<<20, "maximum queued MQTT signaling bytes")
	flag.Parse()

	log.Printf("startup: room=%q signaling=mqtt base-topic=%q", *room, *mqttBaseTopic)
	log.Printf("limits: frame=%d receive=%d send=%d global=%d ice=%d/%d connections=%d pending=%d/%s hello=%.2f/%d mqtt=%d/%d",
		*maxFrameBytes, *receiveBufferBytes, *sendBufferBytes, *maxBufferedBytes,
		*maxPendingICE, *maxPendingICEBytes, *maxConnections, *maxPendingHandshakes, *handshakeTimeout,
		*helloRate, *helloBurst, *mqttQueueMessages, *mqttQueueBytes)
	adapter, err := connectMQTT(*broker, *mqttBaseTopic, "netconn-server", *mqttQueueMessages, *mqttQueueBytes)
	if err != nil {
		log.Fatal(err)
	}
	defer adapter.Disconnect()
	key := sha256.Sum256([]byte(*secret))
	identityKey, err := decodeIdentityKey(*identitySeedHex)
	if err != nil {
		log.Fatal(err)
	}
	listener, err := server.NewListener(
		*room,
		adapter,
		peer.KeyProviderFunc(func(clientID string) ([]byte, error) {
			log.Printf("handshake: authenticating client=%q", clientID)
			return append([]byte(nil), key[:]...), nil
		}),
		&server.Options{
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
		},
		peerapi.WithSTUNServer(
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
		),
	)
	if err != nil {
		log.Fatal(err)
	}
	defer listener.Close()
	log.Printf("ready: listening addr=%s", listener.Addr())

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	go acceptLoop(listener)
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

func acceptLoop(listener net.Listener) {
	for {
		conn, err := listener.Accept()
		if err != nil {
			if !errors.Is(err, net.ErrClosed) {
				log.Printf("connection: accept failed: %v", err)
			}
			return
		}
		go echo(conn)
	}
}

func echo(conn net.Conn) {
	defer conn.Close()
	log.Printf("connection: opened local=%s remote=%s", conn.LocalAddr(), conn.RemoteAddr())
	if _, err := io.Copy(conn, conn); err != nil && !errors.Is(err, net.ErrClosed) {
		log.Printf("data: echo failed remote=%s: %v", conn.RemoteAddr(), err)
	}
	log.Printf("connection: closed remote=%s", conn.RemoteAddr())
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
