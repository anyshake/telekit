package main

import (
	"context"
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"log"
	"sync/atomic"
	"time"

	"github.com/anyshake/telekit/example/p2proxy/common"
	"github.com/anyshake/telekit/example/p2proxy/protocol"
	"github.com/anyshake/telekit/peer"
	peerapi "github.com/anyshake/telekit/peer/api"
	peerclient "github.com/anyshake/telekit/peer/client"
	"github.com/anyshake/telekit/signaling"
	"github.com/anyshake/telekit/transports"
	"github.com/pion/ice/v4"
)

func connectTelekit(args *arguments) (signaling.Adapter, *protocol.Pool, error) {
	if args.poolSize < 1 {
		return nil, nil, fmt.Errorf("pool-size must be positive")
	}
	log.Printf("startup: room=%q signaling=mqtt broker=%q base-topic=%q transport=%q pool-size=%d timeout=%s",
		args.room, args.mqttBroker, args.baseTopic, args.transportName, args.poolSize, args.timeout)
	transport, err := common.NewTransport(args.transportName)
	if err != nil {
		return nil, nil, err
	}
	serverPublicKey, err := decodePublicKey(args.serverPublicKey)
	if err != nil {
		return nil, nil, err
	}
	adapter, err := common.ConnectMQTT(args.mqttBroker, args.baseTopic, "client", args.queueMessages, args.queueBytes)
	if err != nil {
		return nil, nil, err
	}
	log.Printf("signaling: MQTT connected")
	api, err := peerapi.NewAPI(args.room, adapter, common.APIOptions()...)
	if err != nil {
		adapter.Disconnect()
		return nil, nil, err
	}
	key := sha256.Sum256([]byte(args.secret))
	slots := make([]*protocol.SessionSlot, 0, args.poolSize)
	lost := make([]chan struct{}, 0, args.poolSize)
	for index := 0; index < args.poolSize; index++ {
		clientID := args.clientID
		if args.poolSize > 1 {
			clientID = fmt.Sprintf("%s-%d", args.clientID, index)
		}
		log.Printf("peer[%d/%d]: connecting client-id=%q transport=%q", index+1, args.poolSize, clientID, transport.Name())
		lostCh := make(chan struct{}, 1)
		connectCtx, cancel := context.WithTimeout(context.Background(), args.timeout)
		client, err := newPeerClient(connectCtx, api, key[:], serverPublicKey, clientID, transport, args.timeout, lostCh)
		cancel()
		if err != nil {
			for _, slot := range slots {
				slot.Close()
			}
			_ = adapter.Disconnect()
			return nil, nil, err
		}
		slot := protocol.NewSessionSlot()
		slot.Replace(protocol.NewSession(client))
		slots = append(slots, slot)
		lost = append(lost, lostCh)
	}
	pool := protocol.NewPoolSlots(slots...)
	for index, slot := range slots {
		clientID := args.clientID
		if args.poolSize > 1 {
			clientID = fmt.Sprintf("%s-%d", args.clientID, index)
		}
		go superviseSession(pool, slot, lost[index], func(ctx context.Context) (*peerclient.Client, error) {
			return newPeerClient(ctx, api, key[:], serverPublicKey, clientID, transport, args.timeout, lost[index])
		}, args.timeout)
	}
	return adapter, pool, nil
}

func newPeerClient(ctx context.Context, api *peerapi.API, key []byte, serverPublicKey ed25519.PublicKey, clientID string, transport transports.ITransport, timeout time.Duration, lost chan<- struct{}) (*peerclient.Client, error) {
	var phase atomic.Value
	phase.Store("signaling handshake")
	conn, err := peerclient.NewClient(peer.PreSharedKey{
		ClientID:        clientID,
		Key:             key,
		ServerPublicKey: serverPublicKey,
	}, api, &peerclient.Options{
		Timeout:   timeout,
		Transport: transport,
		OnClientHello: func(*peerclient.Client) {
			phase.Store("waiting for server hello")
			log.Printf("peer: client hello sent client-id=%q", clientID)
		},
		OnServerHello: func(*peerclient.Client) {
			phase.Store("transport negotiation")
			log.Printf("peer: server hello received client-id=%q", clientID)
		},
		OnICECandidateGatheringComplete: func(*peerclient.Client) {
			log.Printf("peer: ICE candidate gathering complete client-id=%q", clientID)
		},
		OnICEOffer: func(_ *peerclient.Client, description transports.ICEDescription) {
			phase.Store("waiting for ICE answer")
			log.Printf("peer: ICE offer sent client-id=%q candidates=%d", clientID, len(description.Candidates))
		},
		OnICEAnswer: func(_ *peerclient.Client, description transports.ICEDescription) {
			phase.Store("establishing ICE connection")
			log.Printf("peer: ICE answer received client-id=%q candidates=%d", clientID, len(description.Candidates))
		},
		OnConnected: func(*peerclient.Client) {
			phase.Store("connected")
			log.Printf("transport connected/reconnected: client-id=%s", clientID)
		},
		OnDisconnected: func(*peerclient.Client) {
			log.Printf("peer: transport disconnected client-id=%q", clientID)
			select {
			case lost <- struct{}{}:
			default:
			}
		},
		OnICECandidate: func(_ *peerclient.Client, cd ice.Candidate) {
			log.Printf("exchange candidate: %v", cd.Address())
		},
		OnConnectionFailed: func(_ *peerclient.Client, err error) {
			log.Printf("peer: reconnect failed client-id=%q phase=%s: %v", clientID, phase.Load().(string), err)
		},
	})
	if err != nil {
		return nil, err
	}
	if err := conn.ConnectWithContext(ctx); err != nil {
		log.Printf("peer: connection failed client-id=%q phase=%s: %v", clientID, phase.Load().(string), err)
		_ = conn.Close()
		return nil, fmt.Errorf("peer connection failed for client-id %q during %s: %w", clientID, phase.Load().(string), err)
	}
	return conn, nil
}

func superviseSession(pool *protocol.Pool, slot *protocol.SessionSlot, lost <-chan struct{}, dial func(context.Context) (*peerclient.Client, error), timeout time.Duration) {
	for {
		select {
		case <-pool.Done():
			return
		case <-lost:
		}
		slot.Replace(nil)
		for {
			select {
			case <-pool.Done():
				return
			default:
			}
			ctx, cancel := context.WithTimeout(context.Background(), timeout)
			conn, err := dial(ctx)
			cancel()
			if err == nil {
				slot.Replace(protocol.NewSession(conn))
				break
			}
			log.Printf("pool session reconnect failed: %v", err)
			timer := time.NewTimer(time.Second)
			select {
			case <-timer.C:
			case <-pool.Done():
				timer.Stop()
				return
			}
		}
	}
}

func decodePublicKey(value string) (ed25519.PublicKey, error) {
	key, err := hex.DecodeString(value)
	if err != nil || len(key) != ed25519.PublicKeySize {
		return nil, fmt.Errorf("server public key must be %d-byte hex", ed25519.PublicKeySize)
	}
	return ed25519.PublicKey(key), nil
}
