package protocol

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
)

// Pool distributes independent virtual streams over several authenticated
// sessions. A target TCP connection is still created per stream; sessions are
// not reused as target TCP connections.
type Pool struct {
	sessions  []*Session
	next      atomic.Uint64
	remaining atomic.Int32
	done      chan struct{}
	doneOnce  sync.Once
	closeOnce sync.Once
}

func NewPool(sessions ...*Session) *Pool {
	active := make([]*Session, 0, len(sessions))
	for _, session := range sessions {
		if session != nil {
			active = append(active, session)
		}
	}
	p := &Pool{sessions: active, done: make(chan struct{})}
	p.remaining.Store(int32(len(active)))
	if len(active) == 0 {
		p.signalDone()
		return p
	}
	for _, session := range active {
		go p.watch(session)
	}
	return p
}

func (p *Pool) Done() <-chan struct{} { return p.done }

func (p *Pool) signalDone() {
	p.doneOnce.Do(func() { close(p.done) })
}

func (p *Pool) watch(session *Session) {
	<-session.Done()
	if p.remaining.Add(-1) == 0 {
		p.signalDone()
	}
}

func (p *Pool) Open(ctx context.Context, address string) (*Stream, error) {
	if len(p.sessions) == 0 {
		return nil, errors.New("proxy session pool is empty")
	}
	start := int(p.next.Add(1)-1) % len(p.sessions)
	var lastErr error
	for offset := 0; offset < len(p.sessions); offset++ {
		session := p.sessions[(start+offset)%len(p.sessions)]
		select {
		case <-session.Done():
			lastErr = ErrClosed
			continue
		default:
		}
		stream, err := session.Open(ctx, address)
		if err == nil {
			return stream, nil
		}
		lastErr = err
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}
	}
	if lastErr == nil {
		lastErr = ErrClosed
	}
	return nil, lastErr
}

func (p *Pool) Close() error {
	var firstErr error
	p.closeOnce.Do(func() {
		p.signalDone()
		for _, session := range p.sessions {
			if err := session.Close(); err != nil && firstErr == nil {
				firstErr = err
			}
		}
	})
	return firstErr
}
