package peer

import "fmt"

// Addr implements net.Addr for a room-scoped Telekit endpoint.
type Addr struct {
	RoomID string
	PeerID string
}

func (Addr) Network() string { return "telekit" }

func (a Addr) String() string {
	if a.PeerID == "" {
		return a.RoomID
	}
	return fmt.Sprintf("%s/%s", a.RoomID, a.PeerID)
}
