package transport_quic

import (
	"time"

	quic "github.com/apernet/quic-go"
)

func defaultConfig() *quic.Config {
	return &quic.Config{
		// ICE already gives us a packet-oriented path with a conservative
		// 1200-byte payload. Disable QUIC's UDP socket MTU probing: the socket
		// is an ICE adapter, so probing it cannot reliably discover the real
		// end-to-end path MTU and may black-hole the handshake on weak paths.
		InitialPacketSize:              1200,
		DisablePathMTUDiscovery:        true,
		HandshakeIdleTimeout:           15 * time.Second,
		MaxIdleTimeout:                 5 * time.Minute,
		InitialStreamReceiveWindow:     4 << 20,
		MaxStreamReceiveWindow:         32 << 20,
		InitialConnectionReceiveWindow: 8 << 20,
		MaxConnectionReceiveWindow:     64 << 20,
		EnableDatagrams:                true,
		// The ICE PacketConn is already a single selected path. Path
		// migration only adds bookkeeping and can trigger needless probing.
		DisablePathManager: true,
	}
}
