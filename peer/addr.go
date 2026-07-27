package peer

import (
	"fmt"
	"net"
	"strconv"
)

// Addr implements net.Addr for a room-scoped Telekit endpoint.
type Addr struct {
	RoomID string
	PeerID string
	IP     string
	Port   int
}

func (Addr) Network() string { return "telekit" }

func (a Addr) String() string {
	identity := a.RoomID
	if a.PeerID == "" {
		if a.IP == "" && a.Port == 0 {
			return identity
		}
		return fmt.Sprintf("%s@%s", identity, net.JoinHostPort(a.IP, strconv.Itoa(a.Port)))
	}
	identity = fmt.Sprintf("%s/%s", a.RoomID, a.PeerID)
	if a.IP == "" && a.Port == 0 {
		return identity
	}
	return fmt.Sprintf("%s@%s", identity, net.JoinHostPort(a.IP, strconv.Itoa(a.Port)))
}

// AddrFromNet returns a room-scoped address with the host and port extracted
// from a selected ICE transport address.
func AddrFromNet(roomID, peerID string, address net.Addr) Addr {
	result := Addr{RoomID: roomID, PeerID: peerID}
	if address == nil {
		return result
	}

	switch value := address.(type) {
	case *net.UDPAddr:
		if value != nil {
			result.IP = value.IP.String()
			result.Port = value.Port
		}
	case *net.TCPAddr:
		if value != nil {
			result.IP = value.IP.String()
			result.Port = value.Port
		}
	case *net.IPAddr:
		if value != nil {
			result.IP = value.IP.String()
		}
	default:
		host, port, err := net.SplitHostPort(address.String())
		if err == nil {
			if parsed, parseErr := strconv.Atoi(port); parseErr == nil {
				result.IP = host
				result.Port = parsed
			}
		}
	}
	return result
}
