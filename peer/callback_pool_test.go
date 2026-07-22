package peer

import "testing"

func TestCallbackPoolBoundsQueueAndCancelsPendingOnClose(t *testing.T) {
	pool := NewCallbackPool(1, 1)
	started := make(chan struct{})
	unblock := make(chan struct{})
	if !pool.Submit(func() {
		close(started)
		<-unblock
	}) {
		t.Fatal("running callback rejected")
	}
	<-started
	cancelled := make(chan struct{})
	if !pool.SubmitWithCancel(func() {}, func() { close(cancelled) }) {
		t.Fatal("queued callback rejected")
	}
	if pool.Submit(func() {}) {
		t.Fatal("callback above queue limit accepted")
	}
	pool.Close()
	<-cancelled
	close(unblock)
}
