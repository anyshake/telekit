package peer

import "testing"

func TestReplayWindow(t *testing.T) {
	var window ReplayWindow
	for _, sequence := range []uint64{2, 1, 4, 3, 66, 65} {
		if !window.Accept(sequence) {
			t.Fatalf("fresh sequence %d was rejected", sequence)
		}
		if window.Accept(sequence) {
			t.Fatalf("duplicate sequence %d was accepted", sequence)
		}
	}
	if window.Accept(1) {
		t.Fatal("sequence outside the replay window was accepted")
	}
	if window.Accept(0) {
		t.Fatal("reserved zero sequence was accepted")
	}
	window.Reset()
	if !window.Accept(1) {
		t.Fatal("reset did not clear the replay window")
	}
}
