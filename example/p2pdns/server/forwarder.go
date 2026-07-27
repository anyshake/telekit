package main

import (
	"encoding/binary"
	"errors"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/anyshake/telekit/example/p2pdns/common"
)

type pendingQuery struct {
	conn       net.Conn
	originalID uint16
	expires    time.Time
}

type dnsForwarder struct {
	upstream *net.UDPAddr
	socket   *net.UDPConn
	timeout  time.Duration

	mu        sync.Mutex
	nextID    atomic.Uint32
	pending   map[uint16]pendingQuery
	closeOnce sync.Once
}

func newDNSForwarder(upstream string, timeout time.Duration) (*dnsForwarder, error) {
	addr, err := net.ResolveUDPAddr("udp", upstream)
	if err != nil {
		return nil, fmt.Errorf("invalid upstream DNS address: %w", err)
	}
	if timeout <= 0 {
		timeout = 5 * time.Second
	}
	socket, err := net.ListenUDP("udp", nil)
	if err != nil {
		return nil, err
	}
	return &dnsForwarder{
		upstream: addr, socket: socket, timeout: timeout,
		pending: make(map[uint16]pendingQuery),
	}, nil
}

func (f *dnsForwarder) Run() {
	packet := make([]byte, 64<<10)
	for {
		n, _, err := f.socket.ReadFromUDP(packet)
		if err != nil {
			return
		}
		if n < 2 {
			continue
		}
		id := binary.BigEndian.Uint16(packet[:2])
		query, ok := f.take(id)
		if !ok {
			continue
		}
		binary.BigEndian.PutUint16(packet[:2], query.originalID)
		_ = common.WriteDNSPacket(query.conn, packet[:n])
	}
}

func (f *dnsForwarder) Forward(packet []byte, conn net.Conn) error {
	if len(packet) < 2 {
		return errors.New("DNS packet is too short")
	}
	id := f.reserve(conn, binary.BigEndian.Uint16(packet[:2]))
	query := append([]byte(nil), packet...)
	binary.BigEndian.PutUint16(query[:2], id)
	if _, err := f.socket.WriteToUDP(query, f.upstream); err != nil {
		f.remove(id)
		return err
	}
	return nil
}

func (f *dnsForwarder) reserve(conn net.Conn, originalID uint16) uint16 {
	f.mu.Lock()
	defer f.mu.Unlock()
	now := time.Now()
	for {
		id := uint16(f.nextID.Add(1))
		if query, exists := f.pending[id]; !exists || now.After(query.expires) {
			f.pending[id] = pendingQuery{conn: conn, originalID: originalID, expires: now.Add(f.timeout)}
			return id
		}
	}
}

func (f *dnsForwarder) take(id uint16) (pendingQuery, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	query, ok := f.pending[id]
	if ok {
		delete(f.pending, id)
	}
	if !ok || time.Now().After(query.expires) {
		return pendingQuery{}, false
	}
	return query, true
}

func (f *dnsForwarder) remove(id uint16) {
	f.mu.Lock()
	delete(f.pending, id)
	f.mu.Unlock()
}

func (f *dnsForwarder) Close() error {
	var err error
	f.closeOnce.Do(func() { err = f.socket.Close() })
	return err
}
