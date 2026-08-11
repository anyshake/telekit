package common

import (
	"fmt"

	"github.com/anyshake/telekit/transports"
	transporthttp3 "github.com/anyshake/telekit/transports/transport_http3"
	transportkcp "github.com/anyshake/telekit/transports/transport_kcp"
	transportquic "github.com/anyshake/telekit/transports/transport_quic"
	"github.com/anyshake/telekit/transports/transport_quic/congestion/bbr"
	transportsctp "github.com/anyshake/telekit/transports/transport_sctp"
)

func NewTransport(name string) (transports.ITransport, error) {
	switch name {
	case "quic":
		return transportquic.New(transportquic.WithBBRProfile(bbr.ProfileAggressive)), nil
	case "http3":
		return transporthttp3.New(transporthttp3.WithBBRProfile(bbr.ProfileAggressive)), nil
	case "kcp":
		return newKCPTransport(), nil
	case "sctp":
		return transportsctp.New(), nil
	default:
		return nil, fmt.Errorf("unsupported transport %q", name)
	}
}

func ServerTransports() []transports.ITransport {
	return []transports.ITransport{
		transportquic.New(transportquic.WithBBRProfile(bbr.ProfileAggressive)),
		transporthttp3.New(transporthttp3.WithBBRProfile(bbr.ProfileAggressive)),
		newKCPTransport(),
		transportsctp.New(),
	}
}

func newKCPTransport() *transportkcp.Transport {
	return transportkcp.New(
		transportkcp.WithMTU(1200),
		transportkcp.WithTTI(50),
		transportkcp.WithUplinkCapacity(5),
		transportkcp.WithDownlinkCapacity(30),
		transportkcp.WithCwndMultiplier(20),
		transportkcp.WithMaxSendingWindow(2<<20),
		transportkcp.WithFEC(10, 3),
		transportkcp.WithAdaptiveCongestionControl(true),
		transportkcp.WithCongestionControl(false),
	)
}
