package common

import (
	"fmt"
	"log"
	"time"

	"github.com/anyshake/telekit/signaling"
	"github.com/anyshake/telekit/signaling/mqtt"
)

func ConnectMQTT(endpoint, baseTopic, role string) (signaling.Adapter, error) {
	adapter, err := mqtt.NewMQTTAdapter(
		endpoint,
		mqtt.WithBaseTopic(baseTopic),
		mqtt.WithClientID(fmt.Sprintf("telekit-wsrelay-%s-%d", role, time.Now().UnixNano())),
		mqtt.WithConnectTimeout(10*time.Second),
		mqtt.WithKeepAlive(30*time.Second),
		mqtt.WithPingTimeout(10*time.Second),
		mqtt.WithDispatchQueueLimits(1024, 16<<20),
		mqtt.WithDispatchOverflowHandler(func(topic string) {
			log.Printf("signaling: MQTT queue full; dropped topic=%q", topic)
		}),
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
