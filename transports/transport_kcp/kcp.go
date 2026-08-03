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
// Its default timing and capacity values use a loss-tolerant Xray mKCP
// profile; FEC remains available as an explicit kcp-go option.
type Transport struct {
	// MTU is the KCP payload MTU, excluding the IP and UDP headers.
	MTU int
	// TTI is the KCP update interval in milliseconds.
	TTI int
	// UplinkCapacity and DownlinkCapacity are the estimated capacities in
	// MiB/s, matching Xray mKCP's configuration model.
	UplinkCapacity   uint32
	DownlinkCapacity uint32
	// CwndMultiplier scales the Xray-style control window before it is bounded
	// by MaxSendingWindow.
	CwndMultiplier uint32
	// MaxSendingWindow is the maximum queued sending window in bytes.
	MaxSendingWindow int

	// DataShards and ParityShards retain optional kcp-go FEC support. The
	// defaults use light FEC because ICE paths can lose bursts of packets.
	DataShards   int
	ParityShards int
	// AdaptiveCongestionControl applies the Xray-style control-window response
	// to sustained retransmission/RTO growth. It works with either congestion
	// control mode.
	AdaptiveCongestionControl bool
	// DisableCongestionControl forces kcp-go's no-cwnd mode.
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
	settings := xrayKCPSettings(t)
	configure(session, settings, !t.DisableCongestionControl, t.AdaptiveCongestionControl)
	if _, err := session.Write(sessionPreface); err != nil {
		_ = session.Close()
		return nil, err
	}
	c := &conn{
		session:        session,
		packet:         endpoint.PacketConn,
		local:          endpoint.LocalAddr,
		remote:         endpoint.RemoteAddr,
		writeChunkSize: settings.mtu - 24,
	}
	if t.AdaptiveCongestionControl {
		c.startAdaptiveCongestionControl(settings)
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
		settings := xrayKCPSettings(t)
		configure(session, settings, !t.DisableCongestionControl, t.AdaptiveCongestionControl)
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
		c := &conn{
			session:        session,
			listener:       listener,
			packet:         endpoint.PacketConn,
			local:          endpoint.LocalAddr,
			remote:         endpoint.RemoteAddr,
			writeChunkSize: settings.mtu - 24,
		}
		if t.AdaptiveCongestionControl {
			c.startAdaptiveCongestionControl(settings)
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

func configure(session *kcp.UDPSession, settings kcpSettings, congestionControl, adaptive bool) {
	_ = session.SetMtu(settings.mtu)
	// Xray mKCP uses nodelay, a configurable TTI, and fast resend after two
	// duplicate acknowledgements. The default profile disables kcp-go's native
	// Reno window and uses the delivery-rate controller as the sole controller.
	setCongestionControl(session, settings.tti, congestionControl)
	sendWindow := settings.sendWindow
	if adaptive {
		sendWindow = settings.initialSendWindow
	}
	session.SetWindowSize(sendWindow, settings.receiveWindow)
	session.SetACKNoDelay(true)
	// Flush writes immediately. The ICE path is already selected and KCP's
	// send window provides batching/backpressure; waiting for the next TTI
	// adds up to 50ms per application write and severely limits throughput.
	session.SetWriteDelay(false)
}

func setCongestionControl(session *kcp.UDPSession, interval int, enabled bool) {
	nc := 1
	if enabled {
		nc = 0
	}
	session.SetNoDelay(1, interval, 2, nc)
}

func (c *conn) startAdaptiveCongestionControl(settings kcpSettings) {
	stop := make(chan struct{})
	done := make(chan struct{})
	c.adaptiveStop = stop
	c.adaptiveDone = done
	c.controller = &deliveryRateController{}
	go monitorCongestion(c.session, stop, done, settings, c.controller)
}

func monitorCongestion(session *kcp.UDPSession, stop <-chan struct{}, done chan<- struct{}, settings kcpSettings, controller *deliveryRateController) {
	defer close(done)
	ticker := time.NewTicker(adaptiveCheckInterval)
	defer ticker.Stop()

	var degradedSince time.Time
	window := settings.initialSendWindow
	for {
		select {
		case <-ticker.C:
			now := time.Now()
			rto := session.GetRTO()
			rate := controller.sample(now)
			if rto >= adaptiveFallbackRTO {
				if degradedSince.IsZero() {
					degradedSince = now
				}
				if now.Sub(degradedSince) >= adaptiveFallbackAfter {
					nextWindow := window * 3 / 4
					if nextWindow < minControlWindow {
						nextWindow = minControlWindow
					}
					if nextWindow > settings.sendWindow {
						nextWindow = settings.sendWindow
					}
					if nextWindow < window {
						window = nextWindow
						session.SetWindowSize(window, settings.receiveWindow)
					}
					degradedSince = now
				}
				continue
			}

			degradedSince = time.Time{}

			if rate == 0 || window >= settings.sendWindow {
				continue
			}
			srtt := session.GetSRTT()
			if srtt <= 0 {
				srtt = int32(settings.tti)
			}
			target := bbrTargetWindow(rate, srtt, settings.mtu, settings.sendWindow)
			if target > window {
				step := window / 2
				if step < 1 {
					step = 1
				}
				window += step
				if window > target {
					window = target
				}
				if window > settings.sendWindow {
					window = settings.sendWindow
				}
				session.SetWindowSize(window, settings.receiveWindow)
			}
		case <-stop:
			return
		}
	}
}

type conn struct {
	session        *kcp.UDPSession
	listener       *kcp.Listener
	packet         net.PacketConn
	local          net.Addr
	remote         net.Addr
	writeMu        sync.Mutex
	controller     *deliveryRateController
	writeChunkSize int
	adaptiveStop   chan struct{}
	adaptiveDone   chan struct{}
	closeOnce      sync.Once
	closeErr       error
}

func (c *conn) Read(p []byte) (int, error) { return c.session.Read(p) }
func (c *conn) Write(p []byte) (int, error) {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	chunkSize := c.writeChunkSize
	if chunkSize <= 0 {
		chunkSize = len(p)
	}
	total := 0
	for len(p) > 0 {
		chunk := p
		if len(chunk) > chunkSize {
			chunk = chunk[:chunkSize]
		}
		n, err := c.session.Write(chunk)
		total += n
		if c.controller != nil {
			c.controller.record(n)
		}
		if err != nil || n == 0 {
			return total, err
		}
		p = p[n:]
	}
	return total, nil
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
