package transport_kcp

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"sync"
	"time"

	"github.com/anyshake/telekit/transports"
	kcp "github.com/xtaci/kcp-go/v5"
)

var sessionPreface = []byte("telekit-kcp-v1\x00")

// kcp-go has no stream FIN. The peer layer reserves a zero frame length, so
// this marker lets its reader observe a graceful KCP close as EOF.
var closeMarker = make([]byte, 8)

// Transport uses KCP's stream mode over the packet-preserving ICE connection.
// The defaults use light FEC to prevent recoverable loss from repeatedly
// collapsing KCP's congestion window on weak ICE paths.
type Transport struct {
	MTU          int
	DataShards   int
	ParityShards int
	// AdaptiveCongestionControl falls back to KCP's no-cwnd mode only after
	// sustained loss, then probes congestion control again after recovery.
	AdaptiveCongestionControl bool
	// DisableCongestionControl forces KCP's no-cwnd mode for bulk-throughput
	// links where another layer already controls the sending rate.
	DisableCongestionControl bool
}

func (Transport) Name() string { return "kcp" }

func (t Transport) Dial(_ context.Context, endpoint transports.Endpoint) (net.Conn, error) {
	if endpoint.PacketConn == nil || endpoint.RemoteAddr == nil {
		return nil, errors.New("unsupported transport")
	}
	session, err := kcp.NewConn2(endpoint.RemoteAddr, nil, t.DataShards, t.ParityShards, endpoint.PacketConn)
	if err != nil {
		return nil, err
	}
	configure(session, t.MTU, !t.DisableCongestionControl)
	if _, err := session.Write(sessionPreface); err != nil {
		_ = session.Close()
		return nil, err
	}
	c := &conn{session: session, packet: endpoint.PacketConn, local: endpoint.LocalAddr, remote: endpoint.RemoteAddr}
	if t.AdaptiveCongestionControl && !t.DisableCongestionControl {
		c.startAdaptiveCongestionControl()
	}
	return c, nil
}

func (t Transport) Accept(ctx context.Context, endpoint transports.Endpoint) (net.Conn, error) {
	if endpoint.PacketConn == nil {
		return nil, errors.New("unsupported transport")
	}
	listener, err := kcp.ServeConn(nil, t.DataShards, t.ParityShards, endpoint.PacketConn)
	if err != nil {
		return nil, err
	}
	accepted := make(chan *kcp.UDPSession, 1)
	errs := make(chan error, 1)
	go func() {
		session, err := listener.AcceptKCP()
		if err != nil {
			errs <- err
			return
		}
		accepted <- session
	}()
	select {
	case session := <-accepted:
		configure(session, t.MTU, !t.DisableCongestionControl)
		preface := make([]byte, len(sessionPreface))
		if _, err := io.ReadFull(session, preface); err != nil {
			_ = session.Close()
			_ = listener.Close()
			return nil, err
		}
		if !bytes.Equal(preface, sessionPreface) {
			_ = session.Close()
			_ = listener.Close()
			return nil, errors.New("invalid KCP session preface")
		}
		c := &conn{session: session, listener: listener, packet: endpoint.PacketConn, local: endpoint.LocalAddr, remote: endpoint.RemoteAddr}
		if t.AdaptiveCongestionControl && !t.DisableCongestionControl {
			c.startAdaptiveCongestionControl()
		}
		return c, nil
	case err := <-errs:
		_ = listener.Close()
		return nil, err
	case <-ctx.Done():
		_ = listener.Close()
		_ = endpoint.PacketConn.Close()
		return nil, ctx.Err()
	}
}

func configure(session *kcp.UDPSession, mtu int, congestionControl bool) {
	// Keep packets below the ICE path's conservative MTU. With congestion
	// control enabled, avoiding avoidable fragmentation is more important than
	// using kcp-go's larger default packet size.
	if mtu == 0 {
		mtu = DefaultMTU
	}
	_ = session.SetMtu(mtu)
	// Fast mode: 10 ms interval and aggressive retransmission. nc=0 enables
	// kcp-go's native congestion control; adaptive mode may temporarily change
	// this after sustained loss (see monitorCongestion).
	setCongestionControl(session, congestionControl)
	session.SetWindowSize(1024, 1024)
	session.SetACKNoDelay(true)
	// Coalesce application writes until the next KCP update. This smooths
	// bursts produced by encrypted net.Conn frames without disabling cwnd.
	session.SetWriteDelay(true)
}

func setCongestionControl(session *kcp.UDPSession, enabled bool) {
	nc := 1
	if enabled {
		nc = 0
	}
	session.SetNoDelay(1, 10, 2, nc)
}

func (c *conn) startAdaptiveCongestionControl() {
	stop := make(chan struct{})
	done := make(chan struct{})
	c.adaptiveStop = stop
	c.adaptiveDone = done
	go monitorCongestion(c.session, stop, done)
}

func monitorCongestion(session *kcp.UDPSession, stop <-chan struct{}, done chan<- struct{}) {
	defer close(done)
	ticker := time.NewTicker(adaptiveCheckInterval)
	defer ticker.Stop()

	congestionEnabled := true
	var degradedSince time.Time
	var disabledAt time.Time
	for {
		select {
		case <-ticker.C:
			now := time.Now()
			rto := session.GetRTO()
			if congestionEnabled {
				if rto >= adaptiveFallbackRTO {
					if degradedSince.IsZero() {
						degradedSince = now
					}
					if now.Sub(degradedSince) >= adaptiveFallbackAfter {
						setCongestionControl(session, false)
						congestionEnabled = false
						disabledAt = now
						degradedSince = time.Time{}
					}
				} else {
					degradedSince = time.Time{}
				}
			} else if now.Sub(disabledAt) >= adaptiveRecoveryAfter && rto <= adaptiveRecoveryRTO {
				setCongestionControl(session, true)
				congestionEnabled = true
				disabledAt = time.Time{}
			}
		case <-stop:
			return
		}
	}
}

type conn struct {
	session      *kcp.UDPSession
	listener     *kcp.Listener
	packet       net.PacketConn
	local        net.Addr
	remote       net.Addr
	writeMu      sync.Mutex
	adaptiveStop chan struct{}
	adaptiveDone chan struct{}
	closeOnce    sync.Once
	closeErr     error
}

func (c *conn) Read(p []byte) (int, error) { return c.session.Read(p) }
func (c *conn) Write(p []byte) (int, error) {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	return c.session.Write(p)
}
func (c *conn) LocalAddr() net.Addr                { return c.local }
func (c *conn) RemoteAddr() net.Addr               { return c.remote }
func (c *conn) SetDeadline(t time.Time) error      { return c.session.SetDeadline(t) }
func (c *conn) SetReadDeadline(t time.Time) error  { return c.session.SetReadDeadline(t) }
func (c *conn) SetWriteDeadline(t time.Time) error { return c.session.SetWriteDeadline(t) }
func (c *conn) Close() error {
	c.closeOnce.Do(func() {
		if c.adaptiveStop != nil {
			close(c.adaptiveStop)
			<-c.adaptiveDone
		}
		c.writeMu.Lock()
		if c.session != nil {
			// Keep the packet connection alive long enough for kcp-go's
			// scheduler to put the marker on the ICE path.
			_, _ = c.session.Write(closeMarker)
			time.Sleep(closeMarkerFlushDelay)
			c.closeErr = c.session.Close()
		}
		c.writeMu.Unlock()
		if c.listener != nil {
			if closeErr := c.listener.Close(); c.closeErr == nil {
				c.closeErr = closeErr
			}
		}
		if c.packet != nil {
			if closeErr := c.packet.Close(); c.closeErr == nil {
				c.closeErr = closeErr
			}
		}
	})
	return c.closeErr
}
