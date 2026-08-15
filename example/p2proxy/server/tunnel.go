package main

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
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
		if request.Datagram {
			go serveUDPTunnel(context.Background(), request, pool.resolver.upstream)
			continue
		}
		if !pool.Submit(request) {
			_ = request.Stream.Reject(errors.New("proxy server is busy"))
			_ = request.Stream.Close()
		}
	}
}

func serveUDPTunnel(ctx context.Context, request *protocol.Request, dnsUpstream string) {
	stream := request.Stream
	defer stream.Close()
	packet, err := net.ListenPacket("udp", ":0")
	if err != nil {
		_ = stream.Reject(err)
		return
	}
	defer packet.Close()
	if err := stream.Accept(); err != nil {
		return
	}

	var dnsMu sync.Mutex
	dnsTargets := make(map[uint16][]string)
	errs := make(chan error, 2)
	go func() {
		for {
			target, payload, err := protocol.ReadUDPFrame(stream)
			if err != nil {
				errs <- err
				return
			}
			isDNS, err := isDNSAddress(target)
			if err != nil {
				errs <- err
				return
			}
			relayTarget, err := rewriteDNSAddress(target, dnsUpstream)
			if err != nil {
				errs <- err
				return
			}
			if isDNS && len(payload) >= 2 {
				id := binary.BigEndian.Uint16(payload[:2])
				dnsMu.Lock()
				dnsTargets[id] = append(dnsTargets[id], target)
				dnsMu.Unlock()
			}
			addr, err := net.ResolveUDPAddr("udp", relayTarget)
			if err != nil {
				errs <- err
				return
			}
			if _, err := packet.WriteTo(payload, addr); err != nil {
				errs <- err
				return
			}
		}
	}()
	go func() {
		buffer := make([]byte, 65535)
		for {
			n, addr, err := packet.ReadFrom(buffer)
			if err != nil {
				errs <- err
				return
			}
			udpAddr, ok := addr.(*net.UDPAddr)
			if !ok {
				errs <- fmt.Errorf("unexpected UDP address type %T", addr)
				return
			}
			responseAddr := udpAddr
			if n >= 2 {
				id := binary.BigEndian.Uint16(buffer[:2])
				var originalTarget string
				dnsMu.Lock()
				if targets := dnsTargets[id]; len(targets) > 0 {
					originalTarget = targets[0]
					if len(targets) == 1 {
						delete(dnsTargets, id)
					} else {
						dnsTargets[id] = targets[1:]
					}
				}
				dnsMu.Unlock()
				if originalTarget != "" {
					if originalAddr, resolveErr := net.ResolveUDPAddr("udp", originalTarget); resolveErr == nil {
						responseAddr = originalAddr
					}
				}
			}
			if err := protocol.WriteUDPFrame(stream, responseAddr, buffer[:n]); err != nil {
				errs <- err
				return
			}
		}
	}()

	select {
	case <-ctx.Done():
	case <-errs:
	}
}

func serveRequest(ctx context.Context, request *protocol.Request, resolver *dnsResolver, dialTimeout time.Duration) {
	dialCtx, cancel := context.WithTimeout(ctx, dialTimeout)
	defer cancel()
	target, err := rewriteDNSAddress(request.Address, resolver.upstream)
	if err != nil {
		_ = request.Stream.Reject(err)
		_ = request.Stream.Close()
		return
	}
	remote, err := resolver.dial(dialCtx, target, dialTimeout)
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

func isDNSAddress(address string) (bool, error) {
	_, port, err := net.SplitHostPort(address)
	if err != nil {
		return false, err
	}
	return port == "53", nil
}

func rewriteDNSAddress(address, upstream string) (string, error) {
	isDNS, err := isDNSAddress(address)
	if err != nil {
		return "", err
	}
	if isDNS {
		return upstream, nil
	}
	return address, nil
}
