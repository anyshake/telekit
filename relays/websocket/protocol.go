package websocket

import (
	"encoding/binary"
	"errors"
	"fmt"
	"net"
)

const (
	frameMagic0       = byte('T')
	frameMagic1       = byte('W')
	frameVersion      = byte(1)
	frameHeaderSize   = 22
	maxRelayPacket    = 65535 - frameHeaderSize
	defaultMaxMessage = int64(65535)
)

// RelaySessionID returns the session namespace used by the Telekit peer API.
// The value is opaque to the relay transport; the helper keeps the session
// format consistent between peers and relay authorization code.
func RelaySessionID(namespace, endpointID string) string {
	return namespace + ":" + endpointID
}

var errInvalidFrame = errors.New("invalid websocket relay frame")

func encodeFrame(dst net.Addr, payload []byte) ([]byte, error) {
	udpAddr, ok := dst.(*net.UDPAddr)
	if !ok || udpAddr == nil {
		return nil, fmt.Errorf("relay destination must be *net.UDPAddr, got %T", dst)
	}
	if len(payload) > maxRelayPacket {
		return nil, fmt.Errorf("relay packet is too large: %d", len(payload))
	}

	ip := udpAddr.IP.To4()
	family := byte(4)
	if ip == nil {
		ip = udpAddr.IP.To16()
		family = 6
	}
	if ip == nil || udpAddr.Port < 0 || udpAddr.Port > 65535 {
		return nil, errors.New("invalid relay destination")
	}

	frame := make([]byte, frameHeaderSize+len(payload))
	frame[0] = frameMagic0
	frame[1] = frameMagic1
	frame[2] = frameVersion
	frame[3] = family
	binary.BigEndian.PutUint16(frame[4:6], uint16(udpAddr.Port))
	copy(frame[6:22], ip)
	copy(frame[frameHeaderSize:], payload)

	return frame, nil
}

func decodeFrame(frame []byte) (*net.UDPAddr, []byte, error) {
	if len(frame) < frameHeaderSize || frame[0] != frameMagic0 ||
		frame[1] != frameMagic1 || frame[2] != frameVersion {
		return nil, nil, errInvalidFrame
	}

	port := int(binary.BigEndian.Uint16(frame[4:6]))
	var ip net.IP
	switch frame[3] {
	case 4:
		ip = net.IPv4(frame[6], frame[7], frame[8], frame[9])
	case 6:
		ip = make(net.IP, net.IPv6len)
		copy(ip, frame[6:22])
	default:
		return nil, nil, errInvalidFrame
	}

	return &net.UDPAddr{IP: ip, Port: port}, frame[frameHeaderSize:], nil
}
