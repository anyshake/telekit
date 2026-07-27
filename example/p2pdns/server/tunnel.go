package main

import (
	"errors"
	"net"

	"github.com/anyshake/telekit/example/p2pdns/common"
)

func serveClient(conn net.Conn, forwarder *dnsForwarder) {
	defer conn.Close()
	for {
		packet, err := common.ReadDNSPacket(conn)
		if err != nil {
			return
		}
		if err := forwarder.Forward(packet, conn); err != nil && !errors.Is(err, net.ErrClosed) {
			continue
		}
	}
}
