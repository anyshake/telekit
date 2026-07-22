package peer

import (
	"errors"
	"os"
	"testing"
	"time"
)

func TestRecvBufferDeadline(t *testing.T) {
	buffer := NewRecvBuffer()
	buffer.SetDeadline(time.Now().Add(20 * time.Millisecond))
	_, err := buffer.Read(make([]byte, 1))
	if !errors.Is(err, os.ErrDeadlineExceeded) {
		t.Fatalf("Read error = %v, want deadline exceeded", err)
	}
	buffer.SetDeadline(time.Time{})
	if err := buffer.Write([]byte("x")); err != nil {
		t.Fatal(err)
	}
	p := make([]byte, 1)
	if n, err := buffer.Read(p); n != 1 || err != nil || p[0] != 'x' {
		t.Fatalf("Read = %d, %v, %q", n, err, p)
	}
}

func TestRecvBufferEnforcesLocalAndSharedLimits(t *testing.T) {
	budget := NewByteBudget(5)
	first := NewRecvBufferWithLimit(4, budget)
	second := NewRecvBufferWithLimit(4, budget)
	if err := first.Write([]byte("1234")); err != nil {
		t.Fatal(err)
	}
	if err := second.Write([]byte("12")); !errors.Is(err, ErrReceiveBufferFull) {
		t.Fatalf("shared budget error = %v", err)
	}
	p := make([]byte, 2)
	if _, err := first.Read(p); err != nil {
		t.Fatal(err)
	}
	if err := second.Write([]byte("12")); err != nil {
		t.Fatal(err)
	}
	if err := first.Write([]byte("123")); !errors.Is(err, ErrReceiveBufferFull) {
		t.Fatalf("local limit error = %v", err)
	}
	first.Close()
	second.Close()
	if got := budget.Used(); got != 0 {
		t.Fatalf("budget used = %d, want 0", got)
	}
}
