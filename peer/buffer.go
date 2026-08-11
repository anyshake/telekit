package peer

import (
	"bytes"
	"errors"
	"io"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

const DefaultReceiveBufferSize = 8 << 20

var ErrReceiveBufferFull = errors.New("receive buffer limit exceeded")

// ByteBudget is a concurrency-safe shared memory budget. It is used by a
// server to cap unread application data across all of its connections.
type ByteBudget struct {
	limit int64
	used  atomic.Int64
}

func NewByteBudget(limit int64) *ByteBudget { return &ByteBudget{limit: limit} }

func (b *ByteBudget) Reserve(n int) bool {
	if b == nil || n <= 0 {
		return true
	}
	amount := int64(n)
	for {
		used := b.used.Load()
		if amount > b.limit-used {
			return false
		}
		if b.used.CompareAndSwap(used, used+amount) {
			return true
		}
	}
}

func (b *ByteBudget) Release(n int) {
	if b == nil || n <= 0 {
		return
	}
	if remaining := b.used.Add(-int64(n)); remaining < 0 {
		panic("peer: byte budget released below zero")
	}
}

func (b *ByteBudget) Used() int64 {
	if b == nil {
		return 0
	}
	return b.used.Load()
}

// RecvBuffer is a thread-safe, blocking read buffer that
// delivers complete reassembled messages in order.
// Read blocks until data is available or the buffer is closed.
// Write pushes data in and signals any blocked Read calls.
// Close signals EOF to all blocked Read calls.
type RecvBuffer struct {
	cond     *sync.Cond
	buf      bytes.Buffer
	closed   bool
	deadline time.Time
	limit    int
	budget   *ByteBudget
}

func NewRecvBuffer() *RecvBuffer {
	return NewRecvBufferWithLimit(DefaultReceiveBufferSize, nil)
}

func NewRecvBufferWithLimit(limit int, budget *ByteBudget) *RecvBuffer {
	if limit <= 0 {
		limit = DefaultReceiveBufferSize
	}
	return &RecvBuffer{
		cond:   sync.NewCond(&sync.Mutex{}),
		limit:  limit,
		budget: budget,
	}
}

// Write appends data to the buffer and wakes a waiting Read.
func (rb *RecvBuffer) Write(data []byte) error {
	rb.cond.L.Lock()
	defer rb.cond.L.Unlock()
	if rb.closed {
		return io.ErrClosedPipe
	}
	if len(data) > rb.limit-rb.buf.Len() || !rb.budget.Reserve(len(data)) {
		return ErrReceiveBufferFull
	}
	if _, err := rb.buf.Write(data); err != nil {
		rb.budget.Release(len(data))
		return err
	}
	rb.cond.Signal()
	return nil
}

// Read blocks until data is available or the buffer is closed.
// Returns io.EOF when the buffer has been closed and is empty.
func (rb *RecvBuffer) Read(p []byte) (int, error) {
	rb.cond.L.Lock()
	defer rb.cond.L.Unlock()
	for rb.buf.Len() == 0 {
		if rb.closed {
			return 0, io.EOF
		}
		if !rb.deadline.IsZero() {
			remaining := time.Until(rb.deadline)
			if remaining <= 0 {
				return 0, os.ErrDeadlineExceeded
			}
			timer := time.AfterFunc(remaining, func() {
				rb.cond.L.Lock()
				rb.cond.Broadcast()
				rb.cond.L.Unlock()
			})
			rb.cond.Wait()
			timer.Stop()
			continue
		}
		rb.cond.Wait()
	}
	n, err := rb.buf.Read(p)
	rb.budget.Release(n)
	if rb.buf.Len() == 0 {
		rb.buf = bytes.Buffer{}
	}
	return n, err
}

// Reset discards buffered data while keeping the receive buffer open. It is
// used when a client replaces a failed transport with a newly negotiated one.
func (rb *RecvBuffer) Reset() {
	rb.cond.L.Lock()
	defer rb.cond.L.Unlock()
	if rb.closed {
		return
	}
	buffered := rb.buf.Len()
	rb.buf = bytes.Buffer{}
	rb.budget.Release(buffered)
	rb.cond.Broadcast()
}

func (rb *RecvBuffer) IsClosed() bool {
	rb.cond.L.Lock()
	closed := rb.closed
	rb.cond.L.Unlock()
	return closed
}

func (rb *RecvBuffer) SetDeadline(deadline time.Time) {
	rb.cond.L.Lock()
	rb.deadline = deadline
	rb.cond.Broadcast()
	rb.cond.L.Unlock()
}

// Close marks the buffer as closed and unblocks all pending Read calls.
// Unread data is discarded so shared server memory is released immediately.
// Safe to call multiple times.
func (rb *RecvBuffer) Close() {
	rb.cond.L.Lock()
	defer rb.cond.L.Unlock()
	if rb.closed {
		return
	}
	rb.closed = true
	buffered := rb.buf.Len()
	rb.buf = bytes.Buffer{}
	rb.budget.Release(buffered)
	rb.cond.Broadcast()
}
