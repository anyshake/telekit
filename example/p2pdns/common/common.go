package common

import (
	"fmt"
	"log"
	"time"

	peerapi "github.com/anyshake/telekit/peer/api"
	"github.com/anyshake/telekit/signaling"
	"github.com/anyshake/telekit/signaling/mqtt"
)

const DefaultServerPublicKey = "d75a980182b10ab7d54bfed3c964073a0ee172f3daa62325af021a68f707511a"
const DefaultIdentitySeed = "9d61b19deffd5a60ba844af492ec2cc44449c5697b326919703bac031cae7f60"

func APIOptions() []peerapi.Option {
	return []peerapi.Option{peerapi.WithSTUNServer(
		"stun://stun.l.google.com:19302",
		"stun://stun.cloudflare.com:3478",
		"stun://global.stun.twilio.com:3478",
	)}
}

func ConnectMQTT(endpoint, baseTopic, role string, queueMessages, queueBytes int) (signaling.Adapter, error) {
	adapter, err := mqtt.NewMQTTAdapter(
		endpoint,
		mqtt.WithBaseTopic(baseTopic),
		mqtt.WithClientID(fmt.Sprintf("telekit-p2pdns-%s-%d", role, time.Now().UnixNano())),
		mqtt.WithConnectTimeout(10*time.Second),
		mqtt.WithKeepAlive(30*time.Second),
		mqtt.WithPingTimeout(10*time.Second),
		mqtt.WithDispatchQueueLimits(queueMessages, queueBytes),
		mqtt.WithDispatchOverflowHandler(func(topic string) {
			log.Printf("signaling: MQTT queue full; dropped topic=%q", topic)
		}),
	)
	if err != nil {
		return nil, err
	}
	if err := adapter.Connect(); err != nil {
		return nil, err
	}
	return adapter, nil
}
