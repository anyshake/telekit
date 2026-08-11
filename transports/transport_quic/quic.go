package transport_quic

import (
	"context"
	"errors"
	"io"
	"net"

	"github.com/anyshake/telekit/transports"
	"github.com/anyshake/telekit/transports/transport_quic/congestion/bbr"
	quic "github.com/apernet/quic-go"
)

const protocolName = "telekit-quic"

var streamPreface = []byte("telekit-quic-v1\x00")

type Transport struct {
	Config          *quic.Config
	bbrProfile      bbr.Profile
	brutalBandwidth uint64
}

func (Transport) Name() string { return "quic" }

func (t Transport) Dial(ctx context.Context, endpoint transports.Endpoint) (net.Conn, error) {
	if endpoint.PacketConn == nil || endpoint.RemoteAddr == nil {
		return nil, errors.New("unsupported transport")
	}
	config := t.Config
	if config == nil {
		config = defaultConfig()
	}
	session, err := quic.Dial(ctx, endpoint.PacketConn, endpoint.RemoteAddr, clientTLS(), config)
	if err != nil {
		return nil, err
	}
	InstallCongestionControl(session, congestionRemoteAddr(endpoint), config, t.bbrProfile, t.brutalBandwidth)
	stream, err := session.OpenStreamSync(ctx)
	if err != nil {
		_ = session.CloseWithError(0, "stream open failed")
		return nil, err
	}
	if _, err := stream.Write(streamPreface); err != nil {
		_ = session.CloseWithError(0, "stream preface failed")
		return nil, err
	}
	return &conn{stream: stream, session: session, packet: endpoint.PacketConn, local: endpoint.LocalAddr, remote: endpoint.RemoteAddr}, nil
}

func (t Transport) Accept(ctx context.Context, endpoint transports.Endpoint) (net.Conn, error) {
	if endpoint.PacketConn == nil {
		return nil, errors.New("unsupported transport")
	}
	config := t.Config
	if config == nil {
		config = defaultConfig()
	}
	listener, err := quic.Listen(endpoint.PacketConn, serverTLS(), config)
	if err != nil {
		return nil, err
	}
	session, err := listener.Accept(ctx)
	if err != nil {
		return nil, err
	}
	InstallCongestionControl(session, congestionRemoteAddr(endpoint), config, t.bbrProfile, t.brutalBandwidth)
	stream, err := session.AcceptStream(ctx)
	if err != nil {
		_ = session.CloseWithError(0, "stream accept failed")
		return nil, err
	}
	preface := make([]byte, len(streamPreface))
	if _, err := io.ReadFull(stream, preface); err != nil {
		_ = session.CloseWithError(0, "stream preface read failed")
		return nil, err
	}
	if string(preface) != string(streamPreface) {
		_ = session.CloseWithError(0, "invalid stream preface")
		return nil, io.ErrUnexpectedEOF
	}
	return &conn{stream: stream, session: session, packet: endpoint.PacketConn, local: endpoint.LocalAddr, remote: endpoint.RemoteAddr}, nil
}

func congestionRemoteAddr(endpoint transports.Endpoint) net.Addr {
	// ICEEndpoint keeps the public peer address in Endpoint.RemoteAddr, while
	// Endpoint.Conn still exposes the selected candidate-pair address. BBR's
	// MTU selection needs the latter, just like Xray uses quic.Conn.RemoteAddr.
	if endpoint.Conn != nil {
		if addr := endpoint.Conn.RemoteAddr(); addr != nil {
			return addr
		}
	}
	if addrConn, ok := endpoint.PacketConn.(interface{ RemoteAddr() net.Addr }); ok {
		if addr := addrConn.RemoteAddr(); addr != nil {
			return addr
		}
	}
	return endpoint.RemoteAddr
}
