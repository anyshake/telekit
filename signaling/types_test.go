package signaling

import (
	"strings"
	"testing"
)

func TestRoutingIdentifiersHaveLengthLimits(t *testing.T) {
	if err := ValidateRoomID(strings.Repeat("a", MaxRoomIDLength)); err != nil {
		t.Fatal(err)
	}
	if err := ValidateRoomID(strings.Repeat("a", MaxRoomIDLength+1)); err == nil {
		t.Fatal("oversized room ID accepted")
	}
	if err := ValidateMessageType(MessageType(strings.Repeat("a", MaxMessageTypeLength+1))); err == nil {
		t.Fatal("oversized message type accepted")
	}
}

func TestRoutePrefixValidation(t *testing.T) {
	for _, tc := range []struct {
		prefix    string
		separator rune
		valid     bool
	}{
		{"telekit", '/', true},
		{"sensors/telekit", '/', true},
		{"sensors.telekit", '.', true},
		{"sensors:telekit", ':', true},
		{"sensors/+/telekit", '/', false},
		{"sensors.>.telekit", '.', false},
		{"sensors::telekit", ':', false},
	} {
		if err := ValidateRoutePrefix(tc.prefix, tc.separator); (err == nil) != tc.valid {
			t.Fatalf("ValidateRoutePrefix(%q, %q) = %v, valid=%v", tc.prefix, tc.separator, err, tc.valid)
		}
	}
}
