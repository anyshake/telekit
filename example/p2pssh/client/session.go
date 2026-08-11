package main

import (
	"context"
	"crypto/sha256"
	"fmt"
	"log"
	"net"
	"sync"

	"github.com/anyshake/telekit/example/p2pssh/common"
	"github.com/anyshake/telekit/example/p2pssh/streammux"
	"github.com/anyshake/telekit/peer"
	peerapi "github.com/anyshake/telekit/peer/api"
	"github.com/anyshake/telekit/peer/client"
	"github.com/anyshake/telekit/signaling"
)

type telekitProxy struct {
	mu       sync.Mutex
	cfg      telekitSessionConfig
	clientID string
	adapter  signaling.Adapter
	remote   *client.Client
	mux      *streammux.Session
	done     chan struct{}
	doneOnce sync.Once
}

func (p *telekitProxy) DialStream(ctx context.Context) (net.Conn, error) {
	for attempt := 0; attempt < 2; attempt++ {
		select {
		case <-p.done:
			return nil, streammux.ErrClosed
		default:
		}

		session, err := p.ensureSession(ctx)
		if err != nil {
			return nil, err
		}

		stream, err := session.Dial(ctx)
		if err == nil {
			return stream, nil
		}

		p.resetSession(session)
	}

	return nil, streammux.ErrClosed
}

func (p *telekitProxy) Close() {
	p.doneOnce.Do(func() {
		close(p.done)
	})
	p.resetSession(nil)
}

func (p *telekitProxy) Done() <-chan struct{} {
	return p.done
}

func (p *telekitProxy) ensureSession(ctx context.Context) (*streammux.Session, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	select {
	case <-p.done:
		return nil, streammux.ErrClosed
	default:
	}

	if p.mux != nil && !p.mux.IsClosed() {
		return p.mux, nil
	}

	p.closeLocked()

	remote, adapter, err := dialTelekitSession(
		ctx,
		p.cfg,
		p.clientID,
		"multiplex-session",
	)
	if err != nil {
		return nil, err
	}

	p.remote = remote
	p.adapter = adapter
	p.mux = streammux.NewClient(remote)

	return p.mux, nil
}

func (p *telekitProxy) resetSession(session *streammux.Session) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if session != nil && p.mux != session {
		return
	}

	p.closeLocked()
}

func (p *telekitProxy) closeLocked() {
	if p.mux != nil {
		_ = p.mux.Close()
		p.mux = nil
	}
	if p.remote != nil {
		_ = p.remote.Close()
		p.remote = nil
	}
	if p.adapter != nil {
		p.adapter.Disconnect()
		p.adapter = nil
	}
}

func dialTelekitSession(
	ctx context.Context,
	cfg telekitSessionConfig,
	clientID string,
	localAddr string,
) (*client.Client, signaling.Adapter, error) {
	log.Printf(
		"p2pssh[%s]: connecting MQTT broker=%s client_id=%q",
		localAddr,
		cfg.mqttBroker,
		clientID,
	)

	adapter, err := common.ConnectMQTT(
		cfg.mqttBroker,
		cfg.baseTopic,
		clientID,
		cfg.queueMessages,
		cfg.queueBytes,
	)
	if err != nil {
		return nil, nil, err
	}

	log.Printf(
		"p2pssh[%s]: MQTT connected client_id=%q",
		localAddr,
		clientID,
	)

	if ctx.Err() != nil {
		adapter.Disconnect()
		return nil, nil, ctx.Err()
	}

	api, err := peerapi.NewAPI(
		cfg.room,
		adapter,
		common.APIOptions()...,
	)
	if err != nil {
		adapter.Disconnect()
		return nil, nil, fmt.Errorf("create peer API: %w", err)
	}

	key := sha256.Sum256([]byte(cfg.secret))

	remote, err := client.NewClient(
		peer.PreSharedKey{
			ClientID:        clientID,
			Key:             key[:],
			ServerPublicKey: cfg.serverPublicKey,
		},
		api,
		&client.Options{
			Timeout:            cfg.timeout,
			MaxFrameSize:       cfg.maxFrameBytes,
			ReceiveBufferSize:  cfg.receiveBufferBytes,
			MaxPendingICE:      cfg.maxPendingICE,
			MaxPendingICEBytes: cfg.maxPendingICEBytes,
			Transport:          cfg.transport,
		},
	)
	if err != nil {
		adapter.Disconnect()
		return nil, nil, fmt.Errorf("create Telekit client: %w", err)
	}

	log.Printf(
		"p2pssh[%s]: connecting Telekit session client_id=%q",
		localAddr,
		clientID,
	)

	connectResult := make(chan error, 1)

	go func() {
		connectResult <- remote.Connect()
	}()

	select {
	case <-ctx.Done():
		log.Printf(
			"p2pssh[%s]: Telekit handshake canceled client_id=%q",
			localAddr,
			clientID,
		)

		_ = remote.Close()
		adapter.Disconnect()
		return nil, nil, ctx.Err()

	case err := <-connectResult:
		if err != nil {
			_ = remote.Close()
			adapter.Disconnect()
			return nil, nil, err
		}
	}

	return remote, adapter, nil
}
