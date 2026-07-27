package peer

import (
	"net"
	"testing"
)

func TestAddrStringWithPhysicalAddress(t *testing.T) {
	tests := []struct {
		name string
		addr Addr
		want string
	}{
		{
			name: "legacy",
			addr: Addr{RoomID: "room", PeerID: "peer"},
			want: "room/peer",
		},
		{
			name: "ipv4",
			addr: Addr{RoomID: "room", PeerID: "peer", IP: "192.0.2.10", Port: 3478},
			want: "room/peer@192.0.2.10:3478",
		},
		{
			name: "ipv6",
			addr: Addr{RoomID: "room", PeerID: "peer", IP: "2001:db8::10", Port: 3478},
			want: "room/peer@[2001:db8::10]:3478",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := test.addr.String(); got != test.want {
				t.Fatalf("String() = %q, want %q", got, test.want)
			}
		})
	}
}

func TestAddrFromNet(t *testing.T) {
	addr := AddrFromNet("room", "peer", &net.UDPAddr{IP: net.ParseIP("192.0.2.10"), Port: 3478})
	if addr.IP != "192.0.2.10" || addr.Port != 3478 {
		t.Fatalf("physical address = %q:%d, want 192.0.2.10:3478", addr.IP, addr.Port)
	}
	if got, want := addr.String(), "room/peer@192.0.2.10:3478"; got != want {
		t.Fatalf("String() = %q, want %q", got, want)
	}
}
