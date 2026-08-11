package encryption

import (
	"math"
	"testing"
)

func TestNonceSourceRemainsExhausted(t *testing.T) {
	source, err := newNonceSource(12)
	if err != nil {
		t.Fatal(err)
	}
	source.counter.Store(math.MaxUint64 - 1)
	nonce := make([]byte, 12)
	if err := source.next(nonce); err != nil {
		t.Fatalf("last nonce rejected: %v", err)
	}
	if err := source.next(nonce); err == nil {
		t.Fatal("exhausted nonce source resumed at zero")
	}
	if err := source.next(nonce); err == nil {
		t.Fatal("exhausted nonce source resumed after the first error")
	}
}
