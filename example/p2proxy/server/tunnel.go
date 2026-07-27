package main

import (
	"context"
	"errors"
	"io"
	"net"
	"time"

	"github.com/anyshake/telekit/example/p2proxy/protocol"
)

func serveTunnel(conn net.Conn, pool *requestPool) {
	defer conn.Close()
	session := protocol.NewSession(conn)
	defer session.Close()
	for {
		request, err := session.Accept(context.Background())
		if err != nil {
			return
		}
		if !pool.Submit(request) {
			_ = request.Stream.Reject(errors.New("proxy server is busy"))
			_ = request.Stream.Close()
		}
	}
}

func serveRequest(ctx context.Context, request *protocol.Request, resolver *dnsResolver, dialTimeout time.Duration) {
	dialCtx, cancel := context.WithTimeout(ctx, dialTimeout)
	defer cancel()
	remote, err := resolver.dial(dialCtx, request.Address, dialTimeout)
	if err != nil {
		_ = request.Stream.Reject(err)
		_ = request.Stream.Close()
		return
	}
	defer remote.Close()
	if err := request.Stream.Accept(); err != nil {
		_ = request.Stream.Close()
		return
	}
	stop := make(chan struct{})
	go func() {
		select {
		case <-ctx.Done():
			_ = request.Stream.Close()
			_ = remote.Close()
		case <-stop:
		}
	}()
	defer close(stop)
	responseDone := make(chan struct{})
	go func() {
		defer close(responseDone)
		_, _ = io.Copy(request.Stream, remote)
		_ = request.Stream.CloseWrite()
	}()
	_, copyErr := io.Copy(remote, request.Stream)
	if copyErr != nil && !errors.Is(copyErr, io.EOF) {
		_ = request.Stream.Close()
		return
	}
	<-responseDone
	_ = request.Stream.Close()
}
