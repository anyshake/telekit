package main

import (
	"bufio"
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
	"github.com/anyshake/telekit/peer/client"
	"github.com/anyshake/telekit/signaling"
	"github.com/anyshake/telekit/signaling/mqtt"
)

const demoServerPublicKey = "d75a980182b10ab7d54bfed3c964073a0ee172f3daa62325af021a68f707511a"

func main() {
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)
	log.SetPrefix("[telekit event/client] ")
	broker := flag.String("mqtt", "wss://mqtt-dashboard.com:8884/mqtt", "MQTT broker URL")
	mqttBaseTopic := flag.String("mqtt-base-topic", mqtt.DefaultBaseTopic, "MQTT base topic")
	room := flag.String("room", "event-demo", "room ID")
	clientID := flag.String("client-id", "event-client", "PSK client identity")
	secret := flag.String("secret", "change-me", "pre-shared passphrase")
	serverPublicKeyHex := flag.String("server-public-key", demoServerPublicKey, "pinned Ed25519 server public key (hex)")
	timeout := flag.Duration("timeout", 30*time.Second, "connection timeout")
	compression := flag.Bool("compression", false, "enable zstd compression before encryption")
	maxFrameBytes := flag.Int("max-frame-bytes", 4<<20, "maximum encrypted DataChannel frame size")
	receiveBufferBytes := flag.Int("receive-buffer-bytes", 8<<20, "maximum unread stream data")
	sendBufferBytes := flag.Int("send-buffer-bytes", 1<<20, "DataChannel send-buffer backpressure threshold")
	maxPendingICE := flag.Int("max-pending-ice", 128, "maximum queued ICE Candidates")
	maxPendingICEBytes := flag.Int("max-pending-ice-bytes", 256<<10, "maximum queued ICE Candidate bytes")
	callbackWorkers := flag.Int("callback-workers", 2, "data callback worker count")
	callbackQueue := flag.Int("callback-queue", 64, "maximum queued data callbacks")
	maxCallbackBytes := flag.Int64("max-callback-bytes", 16<<20, "maximum data retained by queued callbacks")
	mqttQueueMessages := flag.Int("mqtt-queue-messages", 1024, "maximum queued MQTT signaling messages")
	mqttQueueBytes := flag.Int("mqtt-queue-bytes", 16<<20, "maximum queued MQTT signaling bytes")
	flag.Parse()

	log.Printf("startup: room=%q signaling=mqtt base-topic=%q", *room, *mqttBaseTopic)
	log.Printf("limits: frame=%d receive=%d send=%d ice=%d/%d callbacks=%d/%d/%d mqtt=%d/%d",
		*maxFrameBytes, *receiveBufferBytes, *sendBufferBytes, *maxPendingICE, *maxPendingICEBytes,
		*callbackWorkers, *callbackQueue, *maxCallbackBytes, *mqttQueueMessages, *mqttQueueBytes)
	adapter, err := connectMQTT(*broker, *mqttBaseTopic, "event-client", *mqttQueueMessages, *mqttQueueBytes)
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
	serverPublicKey, err := decodeServerPublicKey(*serverPublicKeyHex)
	if err != nil {
		log.Fatal(err)
	}
	c, err := client.NewClient(peer.PreSharedKey{
		ClientID:        *clientID,
		Key:             key[:],
		ServerPublicKey: serverPublicKey,
	}, a, &client.Options{
		Timeout:            *timeout,
		UseCompression:     *compression,
		MaxFrameSize:       *maxFrameBytes,
		ReceiveBufferSize:  *receiveBufferBytes,
		MaxSendBufferSize:  *sendBufferBytes,
		MaxPendingICE:      *maxPendingICE,
		MaxPendingICEBytes: *maxPendingICEBytes,
		CallbackWorkers:    *callbackWorkers,
		CallbackQueueSize:  *callbackQueue,
		MaxCallbackBytes:   *maxCallbackBytes,
		ReceiveEventsOnly:  true,
		OnClientHello: func(*client.Client) {
			log.Println("handshake: client hello sent")
		},
		OnServerHello: func(*client.Client) {
			log.Println("handshake: server identity authenticated")
		},
		OnDataChannelOpen: func(conn *client.Client) {
			log.Printf("connection: opened local=%s remote=%s", conn.LocalAddr(), conn.RemoteAddr())
		},
		OnDataChannelMessage: func(_ *client.Client, payload []byte) {
			fmt.Printf("server> %s", payload)
			fmt.Print("> ")
		},
		OnDataChannelClose: func(*client.Client) {
			log.Println("connection: data channel closed")
		},
		OnDisconnected: func(*client.Client) {
			log.Println("connection: disconnected")
		},
		OnConnectionFailed: func(_ *client.Client, err error) {
			log.Printf("connection: failed: %v", err)
		},
		OnDataChannelError: func(_ *client.Client, err error) {
			log.Printf("connection: data channel error: %v", err)
		},
	})
	if err != nil {
		log.Fatal(err)
	}
	defer c.Close()
	if err := c.Connect(); err != nil {
		log.Fatal(err)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	lines := make(chan string)
	go scanLines(lines)
	log.Println("ready: type a line and press Enter; Ctrl-D or Ctrl-C exits")
	fmt.Print("> ")
	for {
		select {
		case <-ctx.Done():
			return
		case line, ok := <-lines:
			if !ok {
				return
			}
			if _, err := c.Write([]byte(line + "\n")); err != nil {
				log.Printf("data: write failed: %v", err)
				return
			}
		}
	}
}

func decodeServerPublicKey(value string) (ed25519.PublicKey, error) {
	key, err := hex.DecodeString(value)
	if err != nil {
		return nil, fmt.Errorf("decode server public key: %w", err)
	}
	if len(key) != ed25519.PublicKeySize {
		return nil, fmt.Errorf("server public key must be %d bytes", ed25519.PublicKeySize)
	}
	return ed25519.PublicKey(key), nil
}

func scanLines(lines chan<- string) {
	defer close(lines)
	scanner := bufio.NewScanner(os.Stdin)
	for scanner.Scan() {
		lines <- scanner.Text()
	}
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
