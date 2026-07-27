package transport_raknet

import (
	"context"
	"encoding/binary"
	"errors"
	"io"
	"net"
	"strings"
	"sync"
	"time"

	"github.com/anyshake/telekit/transports"
	raknet "github.com/sandertv/go-raknet"
)

var streamCloseMarker = []byte("telekit-raknet-close-v1\x00")
var streamDataPrefix = []byte("telekit-raknet-data-v1\x00")

// Transport provides reliable, ordered delivery over the selected ICE path.
// The go-raknet Conn.Write implementation uses RakNet's ReliableOrdered mode.
type Transport struct {
	MaxMTU             uint16
	MaxTransientErrors int
	DisableCookies     bool
	BlockDuration      time.Duration
	// MinWriteInterval and MaxWriteInterval bound the pacing interval between
	// ReliableOrdered packets. RakNet's Conn.Write has no flow-control API, so
	// pacing is needed when the underlying ICE path is slower than the caller.
	MinWriteInterval time.Duration
	MaxWriteInterval time.Duration
	// PacingWindow is the approximate number of packets allowed per measured
	// RTT. A smaller value is safer on lossy/high-latency paths.
	PacingWindow int
}

func (Transport) Name() string { return "raknet" }

func (t Transport) Dial(ctx context.Context, endpoint transports.Endpoint) (net.Conn, error) {
	if err := contextErr(ctx); err != nil {
		return nil, err
	}
	physical, err := newPhysicalEndpoint(endpoint)
	if err != nil {
		return nil, err
	}

	dialer := raknet.Dialer{
		MaxMTU:             t.maxMTU(),
		MaxTransientErrors: t.maxTransientErrors(),
		UpstreamDialer:     fixedDialer{conn: physical.streamConn()},
	}
	session, err := dialer.DialContext(ctx, physical.remote.String())
	if err != nil {
		_ = physical.Close()
		return nil, err
	}
	return newConn(session, physical, nil, endpoint.LocalAddr, endpoint.RemoteAddr, t.writeChunkSize(), t.pacing()), nil
}

func (t Transport) Accept(ctx context.Context, endpoint transports.Endpoint) (net.Conn, error) {
	if err := contextErr(ctx); err != nil {
		return nil, err
	}
	physical, err := newPhysicalEndpoint(endpoint)
	if err != nil {
		return nil, err
	}

	config := raknet.ListenConfig{
		DisableCookies: t.DisableCookies,
		MaxMTU:         t.maxMTU(),
		BlockDuration:  t.BlockDuration,
		UpstreamPacketListener: fixedPacketListener{
			packet: physical,
		},
	}
	listener, err := config.Listen(physical.local.String())
	if err != nil {
		_ = physical.Close()
		return nil, err
	}

	accepted := make(chan struct {
		conn net.Conn
		err  error
	}, 1)
	go func() {
		conn, err := listener.Accept()
		accepted <- struct {
			conn net.Conn
			err  error
		}{conn: conn, err: err}
	}()

	select {
	case result := <-accepted:
		if result.err != nil {
			_ = listener.Close()
			return nil, result.err
		}
		session, ok := result.conn.(*raknet.Conn)
		if !ok {
			_ = listener.Close()
			return nil, errors.New("raknet listener returned an unexpected connection")
		}
		return newConn(session, physical, listener, endpoint.LocalAddr, endpoint.RemoteAddr, t.writeChunkSize(), t.pacing()), nil
	case <-ctx.Done():
		_ = listener.Close()
		return nil, ctx.Err()
	}
}

func (t Transport) maxMTU() uint16 {
	if t.MaxMTU == 0 {
		return DefaultMTU
	}
	return t.MaxMTU
}

func (t Transport) maxTransientErrors() int {
	if t.MaxTransientErrors == 0 {
		return 10
	}
	return t.MaxTransientErrors
}

func (t Transport) writeChunkSize() int {
	mtu := t.maxMTU()
	if mtu < 400 {
		mtu = 400
	}
	// RakNet's effective MTU excludes the IP/UDP header (28 bytes), and a
	// reliable ordered packet needs another 14 bytes of headers. Keeping each
	// stream write at this size avoids go-raknet's split packet path.
	return int(mtu) - 42 - len(streamDataPrefix)
}

func (t Transport) pacing() writePacing {
	minInterval := t.MinWriteInterval
	if minInterval <= 0 {
		minInterval = defaultMinWriteInterval
	}
	maxInterval := t.MaxWriteInterval
	if maxInterval <= 0 {
		maxInterval = defaultMaxWriteInterval
	}
	if maxInterval < minInterval {
		maxInterval = minInterval
	}
	window := t.PacingWindow
	if window <= 0 {
		window = defaultPacingWindow
	}
	return writePacing{
		minInterval: minInterval,
		maxInterval: maxInterval,
		window:      window,
	}
}

func contextErr(ctx context.Context) error {
	if ctx == nil {
		return nil
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
		return nil
	}
}

type physicalEndpoint struct {
	base   net.Conn
	packet net.PacketConn
	local  *net.UDPAddr
	remote *net.UDPAddr
}

func newPhysicalEndpoint(endpoint transports.Endpoint) (*physicalEndpoint, error) {
	if endpoint.Conn == nil && endpoint.PacketConn == nil {
		return nil, errors.New("raknet transport requires an ICE connection")
	}
	localSource := endpoint.LocalAddr
	remoteSource := endpoint.RemoteAddr
	if endpoint.Conn != nil {
		localSource = endpoint.Conn.LocalAddr()
		remoteSource = endpoint.Conn.RemoteAddr()
	}
	_, err := udpAddr(localSource)
	if err != nil {
		return nil, errors.New("raknet transport requires a UDP ICE local address")
	}
	_, err = udpAddr(remoteSource)
	if err != nil {
		return nil, errors.New("raknet transport requires a UDP ICE remote address")
	}
	// go-raknet uses the UDP source address as its connection key. ICE hides
	// that address and a selected pair may also change its representation on
	// reconnect, while this PacketConn is already dedicated to one peer. Use a
	// stable per-endpoint identity for RakNet's internal demux; WriteTo ignores
	// it and continues to send through the selected ICE path.
	identity := &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 19132}
	return &physicalEndpoint{
		base:   endpoint.Conn,
		packet: endpoint.PacketConn,
		local:  identity,
		remote: identity,
	}, nil
}

func udpAddr(addr net.Addr) (*net.UDPAddr, error) {
	udp, ok := addr.(*net.UDPAddr)
	if !ok || udp == nil {
		return nil, errors.New("address is not a UDP address")
	}
	copy := *udp
	copy.IP = append(net.IP(nil), udp.IP...)
	return &copy, nil
}

func (p *physicalEndpoint) streamConn() net.Conn {
	return &datagramConn{endpoint: p}
}

func (p *physicalEndpoint) Read(b []byte) (int, error) {
	if p.base != nil {
		n, err := p.base.Read(b)
		return n, normalizeICEError(err)
	}
	n, _, err := p.packet.ReadFrom(b)
	return n, normalizeICEError(err)
}

func (p *physicalEndpoint) Write(b []byte) (int, error) {
	if p.base != nil {
		n, err := p.base.Write(b)
		return n, normalizeICEError(err)
	}
	n, err := p.packet.WriteTo(b, p.remote)
	return n, normalizeICEError(err)
}

func normalizeICEError(err error) error {
	if err == nil || errors.Is(err, net.ErrClosed) {
		return err
	}
	if strings.Contains(err.Error(), "agent is closed") {
		return net.ErrClosed
	}
	return err
}

func (p *physicalEndpoint) Close() error {
	if p.base != nil {
		return p.base.Close()
	}
	return p.packet.Close()
}

func (p *physicalEndpoint) LocalAddr() net.Addr  { return p.local }
func (p *physicalEndpoint) RemoteAddr() net.Addr { return p.remote }

func (p *physicalEndpoint) SetDeadline(deadline time.Time) error {
	if p.base != nil {
		return p.base.SetDeadline(deadline)
	}
	return p.packet.SetDeadline(deadline)
}

func (p *physicalEndpoint) SetReadDeadline(deadline time.Time) error {
	if p.base != nil {
		return p.base.SetReadDeadline(deadline)
	}
	return p.packet.SetReadDeadline(deadline)
}

func (p *physicalEndpoint) SetWriteDeadline(deadline time.Time) error {
	if p.base != nil {
		return p.base.SetWriteDeadline(deadline)
	}
	return p.packet.SetWriteDeadline(deadline)
}

type datagramConn struct{ endpoint *physicalEndpoint }

func (c *datagramConn) Read(p []byte) (int, error)         { return c.endpoint.Read(p) }
func (c *datagramConn) Write(p []byte) (int, error)        { return c.endpoint.Write(p) }
func (c *datagramConn) Close() error                       { return c.endpoint.Close() }
func (c *datagramConn) LocalAddr() net.Addr                { return c.endpoint.LocalAddr() }
func (c *datagramConn) RemoteAddr() net.Addr               { return c.endpoint.RemoteAddr() }
func (c *datagramConn) SetDeadline(t time.Time) error      { return c.endpoint.SetDeadline(t) }
func (c *datagramConn) SetReadDeadline(t time.Time) error  { return c.endpoint.SetReadDeadline(t) }
func (c *datagramConn) SetWriteDeadline(t time.Time) error { return c.endpoint.SetWriteDeadline(t) }

type fixedDialer struct{ conn net.Conn }

func (d fixedDialer) DialContext(ctx context.Context, _, _ string) (net.Conn, error) {
	if err := contextErr(ctx); err != nil {
		return nil, err
	}
	return &pongFilteringConn{Conn: d.conn}, nil
}

// go-raknet rejects a CONNECTED_PONG when the echoed ping timestamp is ahead
// of the local process uptime. That is valid when the endpoints have different
// start times, and can also happen around a reconnect due to packet reordering.
// Pongs are keepalive acknowledgements only, so discard them at the packet
// boundary and leave application traffic untouched.
type pongFilteringConn struct{ net.Conn }

func (c *pongFilteringConn) Read(p []byte) (int, error) {
	for {
		n, err := c.Conn.Read(p)
		if err != nil {
			return n, err
		}
		if !isConnectedPong(p[:n]) {
			return n, nil
		}
	}
}

type fixedPacketListener struct{ packet *physicalEndpoint }

func (l fixedPacketListener) ListenPacket(_, _ string) (net.PacketConn, error) {
	return &raknetPacketConn{endpoint: l.packet}, nil
}

type raknetPacketConn struct{ endpoint *physicalEndpoint }

func (c *raknetPacketConn) ReadFrom(p []byte) (int, net.Addr, error) {
	for {
		n, err := c.endpoint.Read(p)
		if err != nil {
			return n, c.endpoint.RemoteAddr(), err
		}
		if !isConnectedPong(p[:n]) {
			return n, c.endpoint.RemoteAddr(), nil
		}
	}
}

func (c *raknetPacketConn) WriteTo(p []byte, _ net.Addr) (int, error) {
	return c.endpoint.Write(p)
}

func (c *raknetPacketConn) Close() error                       { return c.endpoint.Close() }
func (c *raknetPacketConn) LocalAddr() net.Addr                { return c.endpoint.LocalAddr() }
func (c *raknetPacketConn) SetDeadline(t time.Time) error      { return c.endpoint.SetDeadline(t) }
func (c *raknetPacketConn) SetReadDeadline(t time.Time) error  { return c.endpoint.SetReadDeadline(t) }
func (c *raknetPacketConn) SetWriteDeadline(t time.Time) error { return c.endpoint.SetWriteDeadline(t) }

func isConnectedPong(b []byte) bool {
	if len(b) < 7 || b[0]&0x80 == 0 || b[0]&0x60 != 0 {
		return false
	}
	for offset := 4; offset < len(b); {
		if len(b)-offset < 3 {
			return false
		}
		header := b[offset]
		reliability := (header & 0xe0) >> 5
		packetHeader := 3
		if reliability == 2 || reliability == 3 || reliability == 4 {
			packetHeader += 3
		}
		if reliability == 1 || reliability == 4 {
			packetHeader += 3
		}
		if reliability == 3 || reliability == 4 {
			packetHeader += 4
		}
		if header&0x10 != 0 {
			packetHeader += 10
		}
		if len(b)-offset < packetHeader {
			return false
		}
		contentLength := int(binary.BigEndian.Uint16(b[offset+1:offset+3]) >> 3)
		end := offset + packetHeader + contentLength
		if end > len(b) {
			return false
		}
		content := b[offset+packetHeader : end]
		if len(content) == 17 && content[0] == 0x03 {
			return true
		}
		offset = end
	}
	return false
}

type readResult struct {
	data []byte
	err  error
}

type conn struct {
	session        *raknet.Conn
	physical       *physicalEndpoint
	listener       *raknet.Listener
	local          net.Addr
	remote         net.Addr
	writeChunkSize int
	pacing         writePacing

	readMu      sync.Mutex
	readPending []byte
	readEOF     bool
	readErr     error
	readCh      chan readResult
	stopRead    chan struct{}
	stopOnce    sync.Once

	writeMu         sync.Mutex
	nextWriteAt     time.Time
	closeSignal     chan struct{}
	closeSignalOnce sync.Once
	deadlineMu      sync.RWMutex
	readDeadline    time.Time
	writeDeadline   time.Time

	closeOnce sync.Once
	closeErr  error
}

func newConn(session *raknet.Conn, physical *physicalEndpoint, listener *raknet.Listener, local, remote net.Addr, writeChunkSize int, pacing writePacing) *conn {
	if local == nil {
		local = physical.LocalAddr()
	}
	if remote == nil {
		remote = physical.RemoteAddr()
	}
	c := &conn{
		session:        session,
		physical:       physical,
		listener:       listener,
		local:          local,
		remote:         remote,
		writeChunkSize: writeChunkSize,
		pacing:         pacing,
		readCh:         make(chan readResult, 8),
		stopRead:       make(chan struct{}),
		closeSignal:    make(chan struct{}),
	}
	go c.readLoop()
	return c
}

func (c *conn) readLoop() {
	for {
		data, err := c.session.ReadPacket()
		if err == nil {
			if bytesEqual(data, streamCloseMarker) {
				// Preserve the close marker for the net.Conn reader.
			} else if len(data) >= len(streamDataPrefix) && bytesEqual(data[:len(streamDataPrefix)], streamDataPrefix) {
				data = data[len(streamDataPrefix):]
			} else {
				// RakNet control packets are consumed before ReadPacket. Ignore
				// anything else to keep the stream framing unambiguous.
				continue
			}
		}
		result := readResult{data: data, err: err}
		select {
		case c.readCh <- result:
		case <-c.stopRead:
			return
		}
		if err != nil {
			return
		}
	}
}

func (c *conn) Read(p []byte) (int, error) {
	c.readMu.Lock()
	defer c.readMu.Unlock()
	for {
		if len(c.readPending) != 0 {
			n := copy(p, c.readPending)
			c.readPending = c.readPending[n:]
			return n, nil
		}
		if c.readEOF {
			return 0, io.EOF
		}
		if c.readErr != nil {
			return 0, c.readErr
		}

		deadline := c.getReadDeadline()
		var timer *time.Timer
		var timeout <-chan time.Time
		if !deadline.IsZero() {
			timer = time.NewTimer(time.Until(deadline))
			timeout = timer.C
		}
		select {
		case result := <-c.readCh:
			if timer != nil {
				timer.Stop()
			}
			if len(result.data) == 0 && result.err == nil {
				continue
			}
			if bytesEqual(result.data, streamCloseMarker) {
				c.readEOF = true
				continue
			}
			if len(result.data) != 0 {
				c.readPending = result.data
			}
			if result.err != nil {
				c.readErr = result.err
			}
		case <-timeout:
			return 0, timeoutError{}
		case <-c.stopRead:
			return 0, net.ErrClosed
		}
	}
}

func (c *conn) Write(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	if deadline := c.getWriteDeadline(); !deadline.IsZero() && !time.Now().Before(deadline) {
		return 0, timeoutError{}
	}
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	select {
	case <-c.closeSignal:
		return 0, net.ErrClosed
	default:
	}
	chunkSize := c.writeChunkSize
	if chunkSize <= 0 {
		chunkSize = len(p)
	}
	total := 0
	for total < len(p) {
		end := min(total+chunkSize, len(p))
		if err := c.waitForWritePace(); err != nil {
			return total, err
		}
		packet := make([]byte, len(streamDataPrefix)+end-total)
		copy(packet, streamDataPrefix)
		copy(packet[len(streamDataPrefix):], p[total:end])
		n, err := c.session.Write(packet)
		if err != nil {
			if n > len(streamDataPrefix) {
				total += min(n-len(streamDataPrefix), end-total)
			}
			return total, err
		}
		if n != len(packet) {
			if n > len(streamDataPrefix) {
				total += min(n-len(streamDataPrefix), end-total)
			}
			return total, io.ErrShortWrite
		}
		total = end
	}
	return total, nil
}

// waitForWritePace provides the backpressure missing from go-raknet's public
// API. Without it, a bulk io.Copy can enqueue thousands of ReliableOrdered
// packets in the retransmission map. Loss then causes retransmissions and
// head-of-line blocking to grow over time, making the effective throughput
// continually worse on a weak ICE path.
func (c *conn) waitForWritePace() error {
	select {
	case <-c.closeSignal:
		return net.ErrClosed
	default:
	}
	now := time.Now()
	if !c.nextWriteAt.IsZero() && now.Before(c.nextWriteAt) {
		wait := time.NewTimer(time.Until(c.nextWriteAt))
		defer wait.Stop()
		deadline := c.getWriteDeadline()
		if deadline.IsZero() {
			select {
			case <-wait.C:
			case <-c.closeSignal:
				return net.ErrClosed
			case <-c.stopRead:
				return net.ErrClosed
			}
		} else {
			remaining := time.Until(deadline)
			if remaining <= 0 {
				return timeoutError{}
			}
			deadlineTimer := time.NewTimer(remaining)
			defer deadlineTimer.Stop()
			select {
			case <-wait.C:
			case <-deadlineTimer.C:
				return timeoutError{}
			case <-c.closeSignal:
				return net.ErrClosed
			case <-c.stopRead:
				return net.ErrClosed
			}
		}
	}

	interval := c.pacing.minInterval
	if latency := c.session.Latency(); latency > 0 {
		// Latency() is half of the measured RTT. Pacing at 2*latency/window
		// therefore targets approximately window packets per RTT.
		interval = 2 * latency / time.Duration(c.pacing.window)
		if interval < c.pacing.minInterval {
			interval = c.pacing.minInterval
		}
		if interval > c.pacing.maxInterval {
			interval = c.pacing.maxInterval
		}
	}
	c.nextWriteAt = time.Now().Add(interval)
	return nil
}

type writePacing struct {
	minInterval time.Duration
	maxInterval time.Duration
	window      int
}

func (c *conn) LocalAddr() net.Addr  { return c.local }
func (c *conn) RemoteAddr() net.Addr { return c.remote }

func (c *conn) SetDeadline(deadline time.Time) error {
	c.deadlineMu.Lock()
	c.readDeadline = deadline
	c.writeDeadline = deadline
	c.deadlineMu.Unlock()
	return nil
}

func (c *conn) SetReadDeadline(deadline time.Time) error {
	c.deadlineMu.Lock()
	c.readDeadline = deadline
	c.deadlineMu.Unlock()
	return nil
}

func (c *conn) SetWriteDeadline(deadline time.Time) error {
	c.deadlineMu.Lock()
	c.writeDeadline = deadline
	c.deadlineMu.Unlock()
	return nil
}

func (c *conn) getReadDeadline() time.Time {
	c.deadlineMu.RLock()
	deadline := c.readDeadline
	c.deadlineMu.RUnlock()
	return deadline
}

func (c *conn) getWriteDeadline() time.Time {
	c.deadlineMu.RLock()
	deadline := c.writeDeadline
	c.deadlineMu.RUnlock()
	return deadline
}

func (c *conn) Close() error {
	c.closeSignalOnce.Do(func() { close(c.closeSignal) })
	c.closeOnce.Do(func() {
		c.writeMu.Lock()
		_, _ = c.session.Write(streamCloseMarker)
		c.writeMu.Unlock()
		c.stopOnce.Do(func() { close(c.stopRead) })
		// Give the reliable ordered packet one scheduling interval to reach the
		// peer before tearing down the ICE endpoint.
		time.Sleep(closeMarkerFlushDelay)
		c.closeErr = c.session.Close()
		if c.listener != nil {
			if err := c.listener.Close(); c.closeErr == nil {
				c.closeErr = err
			}
		} else if err := c.physical.Close(); c.closeErr == nil {
			c.closeErr = err
		}
	})
	return c.closeErr
}

type timeoutError struct{}

func (timeoutError) Error() string   { return "i/o timeout" }
func (timeoutError) Timeout() bool   { return true }
func (timeoutError) Temporary() bool { return true }

func bytesEqual(a, b []byte) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

var _ transports.ITransport = (*Transport)(nil)
var _ net.Conn = (*conn)(nil)
