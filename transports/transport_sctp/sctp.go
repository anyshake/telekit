package transport_sctp

import (
	"context"
	"errors"
	"io"
	"net"
	"sync"
	"time"

	"github.com/anyshake/telekit/transports"
	"github.com/pion/datachannel"
	"github.com/pion/logging"
	"github.com/pion/sctp"
)

type Transport struct {
	MTU                uint32
	MaxReceiveBuffer   uint32
	MaxMessageSize     uint32
	EnableInterleaving bool
	BlockWrite         bool
	RTOMax             float64
	MinCwnd            uint32
	FastRtxWnd         uint32
	CwndCAStep         uint32
}

func (Transport) Name() string { return "sctp" }

func (t Transport) Dial(ctx context.Context, endpoint transports.Endpoint) (net.Conn, error) {
	if endpoint.Conn == nil {
		return nil, errors.New("unsupported transport")
	}
	association, err := clientAssociation(ctx, endpoint.Conn, t)
	if err != nil {
		return nil, err
	}
	channel, err := datachannel.Dial(association, 0, &datachannel.Config{
		ChannelType:   datachannel.ChannelTypeReliable,
		Label:         "telekit",
		LoggerFactory: logging.NewDefaultLoggerFactory(),
	})
	if err != nil {
		_ = association.Close()
		return nil, err
	}
	return newConn(channel, association, endpoint.LocalAddr, endpoint.RemoteAddr, t.MaxMessageSize), nil
}

func (t Transport) Accept(ctx context.Context, endpoint transports.Endpoint) (net.Conn, error) {
	if endpoint.Conn == nil {
		return nil, errors.New("unsupported transport")
	}
	association, err := serverAssociation(ctx, endpoint.Conn, t)
	if err != nil {
		return nil, err
	}
	channel, err := acceptChannel(ctx, association)
	if err != nil {
		_ = association.Close()
		return nil, err
	}
	return newConn(channel, association, endpoint.LocalAddr, endpoint.RemoteAddr, t.MaxMessageSize), nil
}

func clientAssociation(ctx context.Context, conn net.Conn, t Transport) (*sctp.Association, error) {
	return createAssociation(ctx, conn, t, true)
}

func serverAssociation(ctx context.Context, conn net.Conn, t Transport) (*sctp.Association, error) {
	return createAssociation(ctx, conn, t, false)
}

func createAssociation(ctx context.Context, conn net.Conn, t Transport, client bool) (*sctp.Association, error) {
	result := make(chan struct {
		association *sctp.Association
		err         error
	}, 1)
	go func() {
		mtu := t.MTU
		if mtu == 0 {
			mtu = DefaultMTU
		}
		maxReceiveBuffer := t.MaxReceiveBuffer
		if maxReceiveBuffer == 0 {
			maxReceiveBuffer = DefaultMaxReceiveBuffer
		}
		maxMessageSize := t.MaxMessageSize
		if maxMessageSize == 0 {
			maxMessageSize = DefaultMaxMessageSize
		}
		rtoMax := t.RTOMax
		if rtoMax == 0 {
			rtoMax = float64(DefaultRTOMax)
		}
		minCwnd := t.MinCwnd
		if minCwnd == 0 {
			minCwnd = mtu * 8
		}
		fastRtxWnd := t.FastRtxWnd
		if fastRtxWnd == 0 {
			fastRtxWnd = mtu * 8
		}
		cwndCAStep := t.CwndCAStep
		if cwndCAStep == 0 {
			cwndCAStep = mtu * 2
		}
		opts := []sctp.AssociationOption{
			sctp.WithNetConn(conn),
			sctp.WithName("telekit-sctp"),
			sctp.WithBlockWrite(t.BlockWrite),
			sctp.WithEnableInterleaving(t.EnableInterleaving),
			sctp.WithMTU(mtu),
			sctp.WithRTOMax(rtoMax),
			sctp.WithMinCwnd(minCwnd),
			sctp.WithFastRtxWnd(fastRtxWnd),
			sctp.WithCwndCAStep(cwndCAStep),
			sctp.WithMaxReceiveBufferSize(maxReceiveBuffer),
			sctp.WithMaxMessageSize(maxMessageSize),
		}
		var association *sctp.Association
		var err error
		if client {
			clientOpts := make([]sctp.ClientOption, len(opts))
			for i, opt := range opts {
				clientOpts[i] = opt
			}
			association, err = sctp.ClientWithOptions(clientOpts...)
		} else {
			serverOpts := make([]sctp.ServerOption, len(opts))
			for i, opt := range opts {
				serverOpts[i] = opt
			}
			association, err = sctp.ServerWithOptions(serverOpts...)
		}
		result <- struct {
			association *sctp.Association
			err         error
		}{association, err}
	}()
	select {
	case result := <-result:
		return result.association, result.err
	case <-ctx.Done():
		_ = conn.Close()
		return nil, ctx.Err()
	}
}

func acceptChannel(ctx context.Context, association *sctp.Association) (*datachannel.DataChannel, error) {
	result := make(chan struct {
		channel *datachannel.DataChannel
		err     error
	}, 1)
	go func() {
		channel, err := datachannel.Accept(association, &datachannel.Config{
			Label:         "telekit",
			LoggerFactory: logging.NewDefaultLoggerFactory(),
		})
		result <- struct {
			channel *datachannel.DataChannel
			err     error
		}{channel, err}
	}()
	select {
	case result := <-result:
		return result.channel, result.err
	case <-ctx.Done():
		_ = association.Close()
		return nil, ctx.Err()
	}
}

type conn struct {
	channel        *datachannel.DataChannel
	association    *sctp.Association
	local          net.Addr
	remote         net.Addr
	maxMessageSize int
	readMu         sync.Mutex
	pending        []byte
	closeOnce      sync.Once
	closeErr       error
}

func newConn(channel *datachannel.DataChannel, association *sctp.Association, local, remote net.Addr, maxSize uint32) *conn {
	if maxSize == 0 {
		maxSize = DefaultMaxMessageSize
	}
	return &conn{channel: channel, association: association, local: local, remote: remote, maxMessageSize: int(maxSize)}
}

func (c *conn) Read(p []byte) (int, error) {
	c.readMu.Lock()
	defer c.readMu.Unlock()
	if len(c.pending) > 0 {
		n := copy(p, c.pending)
		c.pending = c.pending[n:]
		return n, nil
	}
	message := make([]byte, c.maxMessageSize)
	n, err := c.channel.Read(message)
	if err != nil {
		return 0, err
	}
	if n == 0 {
		return 0, nil
	}
	count := copy(p, message[:n])
	if count < n {
		c.pending = append(c.pending, message[count:n]...)
	}
	return count, nil
}

func (c *conn) Write(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	for offset := 0; offset < len(p); {
		end := offset + c.maxMessageSize
		if end > len(p) {
			end = len(p)
		}
		n, err := c.channel.Write(p[offset:end])
		offset += n
		if err != nil {
			return offset, err
		}
		if n == 0 {
			return offset, io.ErrShortWrite
		}
	}
	return len(p), nil
}

func (c *conn) LocalAddr() net.Addr  { return c.local }
func (c *conn) RemoteAddr() net.Addr { return c.remote }
func (c *conn) SetDeadline(t time.Time) error {
	if err := c.SetReadDeadline(t); err != nil {
		return err
	}
	return c.SetWriteDeadline(t)
}
func (c *conn) SetReadDeadline(t time.Time) error  { return c.channel.SetReadDeadline(t) }
func (c *conn) SetWriteDeadline(t time.Time) error { return c.channel.SetWriteDeadline(t) }
func (c *conn) Close() error {
	c.closeOnce.Do(func() {
		if err := c.channel.Close(); err != nil && !errors.Is(err, io.EOF) {
			c.closeErr = err
		}
		// association.Close aborts the underlying net.Conn immediately. Give
		// SCTP a short graceful shutdown window first so the peer's DataChannel
		// reader receives EOF and does not leave a stale server session.
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		_ = c.association.Shutdown(ctx)
		cancel()
		if err := c.association.Close(); c.closeErr == nil {
			c.closeErr = err
		}
	})
	return c.closeErr
}
