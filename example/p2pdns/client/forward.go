package main

import (
	"context"
	"encoding/binary"
	"errors"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/anyshake/telekit/example/p2pdns/common"
)

type pendingDNS struct {
	addr       *net.UDPAddr
	originalID uint16
	expires    time.Time
}

type dnsClient struct {
	local   *net.UDPConn
	remote  net.Conn
	timeout time.Duration

	mu        sync.Mutex
	nextID    atomic.Uint32
	pending   map[uint16]pendingDNS
	closeOnce sync.Once
}

func newDNSClient(local *net.UDPConn, remote net.Conn, timeout time.Duration) *dnsClient {
	if timeout <= 0 {
		timeout = 5 * time.Second
	}
	return &dnsClient{
		local: local, remote: remote, timeout: timeout,
		pending: make(map[uint16]pendingDNS),
	}
}

func (c *dnsClient) Run(ctx context.Context) error {
	remoteDone := make(chan struct{})
	go func() {
		defer close(remoteDone)
		c.readRemote()
	}()
	go func() {
		select {
		case <-ctx.Done():
		case <-remoteDone:
		}
		_ = c.Close()
	}()
	for {
		packet := make([]byte, 64<<10)
		n, addr, err := c.local.ReadFromUDP(packet)
		if err != nil {
			if errors.Is(err, net.ErrClosed) || ctx.Err() != nil {
				return nil
			}
			if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
				continue
			}
			return err
		}
		if n < 2 {
			continue
		}
		id := c.reserveID(addr, binary.BigEndian.Uint16(packet[:2]))
		binary.BigEndian.PutUint16(packet[:2], id)
		if err := common.WriteDNSPacket(c.remote, packet[:n]); err != nil {
			c.remove(id)
		}
		select {
		case <-remoteDone:
			return nil
		default:
		}
	}
}

func (c *dnsClient) readRemote() {
	for {
		packet, err := common.ReadDNSPacket(c.remote)
		if err != nil {
			return
		}
		if len(packet) < 2 {
			continue
		}
		id := binary.BigEndian.Uint16(packet[:2])
		pending, ok := c.take(id)
		if !ok {
			continue
		}
		binary.BigEndian.PutUint16(packet[:2], pending.originalID)
		_, _ = c.local.WriteToUDP(packet, pending.addr)
	}
}

func (c *dnsClient) reserveID(addr *net.UDPAddr, originalID uint16) uint16 {
	c.mu.Lock()
	defer c.mu.Unlock()
	now := time.Now()
	for {
		id := uint16(c.nextID.Add(1))
		if pending, exists := c.pending[id]; !exists || now.After(pending.expires) {
			c.pending[id] = pendingDNS{addr: addr, originalID: originalID, expires: now.Add(c.timeout)}
			return id
		}
	}
}

func (c *dnsClient) take(id uint16) (pendingDNS, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	pending, ok := c.pending[id]
	if ok {
		delete(c.pending, id)
	}
	if !ok || time.Now().After(pending.expires) {
		return pendingDNS{}, false
	}
	return pending, true
}

func (c *dnsClient) remove(id uint16) {
	c.mu.Lock()
	delete(c.pending, id)
	c.mu.Unlock()
}

func (c *dnsClient) Close() error {
	var firstErr error
	c.closeOnce.Do(func() {
		if err := c.local.Close(); err != nil {
			firstErr = err
		}
		if err := c.remote.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	})
	return firstErr
}
