package transport_quic

import (
	"context"
	"errors"
	"io"
	"net"

	"github.com/anyshake/telekit/transports"
	quic "github.com/apernet/quic-go"
)

const protocolName = "telekit-quic"

var streamPreface = []byte("telekit-quic-v1\x00")

type Transport struct {
	Config *quic.Config
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
	installCongestionControl(session, endpoint.RemoteAddr)
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
	installCongestionControl(session, endpoint.RemoteAddr)
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
