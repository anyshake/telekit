package main

import (
	"context"
	"io"
	"log"
	"net"
	"sync"
)

type proxyCopyResult struct {
	direction string
	bytes     int64
	err       error
}

func proxyConn(
	ctx context.Context,
	local net.Conn,
	clientID string,
	proxy *telekitProxy,
) {
	defer local.Close()

	if ctx.Err() != nil {
		return
	}

	localAddr := local.RemoteAddr().String()

	log.Printf(
		"p2pssh[%s]: connecting Telekit client_id=%q",
		localAddr,
		clientID,
	)

	remote, err := proxy.DialStream(ctx)
	if err != nil {
		log.Printf(
			"p2pssh[%s]: Telekit connect failed client_id=%q: %T: %v",
			localAddr,
			clientID,
			err,
			err,
		)
		return
	}
	defer remote.Close()

	log.Printf(
		"p2pssh[%s]: Telekit session established client_id=%q",
		localAddr,
		clientID,
	)

	proxyConnWithRemote(
		ctx,
		local,
		remote,
		clientID,
	)
}

func proxyConnWithRemote(
	ctx context.Context,
	local net.Conn,
	remote net.Conn,
	clientID string,
) {
	localAddr := local.RemoteAddr().String()

	log.Printf(
		"p2pssh[%s]: forwarding traffic client_id=%q",
		localAddr,
		clientID,
	)

	var closeOnce sync.Once

	closeBoth := func() {
		closeOnce.Do(func() {
			_ = local.Close()
			_ = remote.Close()
		})
	}
	defer closeBoth()

	results := make(chan proxyCopyResult, 2)

	copyData := func(
		direction string,
		dst net.Conn,
		src net.Conn,
	) {
		n, err := io.Copy(dst, src)

		results <- proxyCopyResult{
			direction: direction,
			bytes:     n,
			err:       err,
		}
	}

	go copyData(
		"telekit -> local",
		local,
		remote,
	)

	go copyData(
		"local -> telekit",
		remote,
		local,
	)

	firstResultReceived := false

	select {
	case <-ctx.Done():
		log.Printf(
			"p2pssh[%s]: closing active session client_id=%q: %v",
			localAddr,
			clientID,
			ctx.Err(),
		)

	case result := <-results:
		firstResultReceived = true

		logCopyResult(
			localAddr,
			clientID,
			result.direction,
			result.bytes,
			result.err,
			ctx.Err(),
		)
	}

	closeBoth()

	if firstResultReceived {
		result := <-results

		logCopyResult(
			localAddr,
			clientID,
			result.direction,
			result.bytes,
			result.err,
			ctx.Err(),
		)
	} else {
		for range 2 {
			result := <-results

			logCopyResult(
				localAddr,
				clientID,
				result.direction,
				result.bytes,
				result.err,
				ctx.Err(),
			)
		}
	}

	log.Printf(
		"p2pssh[%s]: session closed client_id=%q",
		localAddr,
		clientID,
	)
}
