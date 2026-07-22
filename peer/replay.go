package peer

import "sync"

// ReplayWindow accepts each non-zero sequence number once while allowing
// bounded out-of-order delivery. It is safe for concurrent signaling handlers.
type ReplayWindow struct {
	mu      sync.Mutex
	highest uint64
	seen    uint64
}

func (w *ReplayWindow) Accept(sequence uint64) bool {
	if sequence == 0 {
		return false
	}

	w.mu.Lock()
	defer w.mu.Unlock()

	if w.highest == 0 {
		w.highest = sequence
		w.seen = 1
		return true
	}
	if sequence > w.highest {
		shift := sequence - w.highest
		if shift >= 64 {
			w.seen = 0
		} else {
			w.seen <<= shift
		}
		w.highest = sequence
		w.seen |= 1
		return true
	}

	delta := w.highest - sequence
	if delta >= 64 {
		return false
	}
	mask := uint64(1) << delta
	if w.seen&mask != 0 {
		return false
	}
	w.seen |= mask
	return true
}

func (w *ReplayWindow) Reset() {
	w.mu.Lock()
	w.highest = 0
	w.seen = 0
	w.mu.Unlock()
}
