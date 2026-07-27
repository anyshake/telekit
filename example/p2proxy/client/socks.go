package main

import (
	"context"
	"io"
	"net"
	"time"

	"github.com/anyshake/telekit/example/p2proxy/protocol"
)

func acceptSOCKS(ctx context.Context, listener net.Listener, pool *protocol.Pool) {
	for {
		conn, err := listener.Accept()
		if err != nil {
			return
		}
		go handleSOCKS(ctx, conn, pool)
	}
}

func handleSOCKS(ctx context.Context, local net.Conn, pool *protocol.Pool) {
	defer local.Close()
	if err := socksHandshake(local); err != nil {
		return
	}
	target, err := readSOCKSRequest(local)
	if err != nil {
		_ = writeSOCKSReply(local, 1)
		return
	}
	openCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	stream, err := pool.Open(openCtx, target)
	cancel()
	if err != nil {
		_ = writeSOCKSReply(local, 5)
		return
	}
	if err := writeSOCKSReply(local, 0); err != nil {
		_ = stream.Close()
		return
	}
	go func() {
		_, _ = io.Copy(stream, local)
		_ = stream.CloseWrite()
	}()
	_, _ = io.Copy(local, stream)
	_ = stream.Close()
}
