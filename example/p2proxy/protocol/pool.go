package protocol

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
)

// Pool distributes independent virtual streams over several authenticated
// sessions. A target TCP connection is still created per stream; sessions
// are not reused as target TCP connections.
type Pool struct {
	slots     []*SessionSlot
	next      atomic.Uint64
	done      chan struct{}
	doneOnce  sync.Once
	closeOnce sync.Once
	changed   chan struct{}
}

// SessionSlot owns one logical pool member. Its session can be replaced when
// the peer client is rebuilt after a failed ICE connection.
type SessionSlot struct {
	mu       sync.RWMutex
	session  *Session
	closed   bool
	onChange func()
}

func NewSessionSlot() *SessionSlot { return &SessionSlot{} }

func (s *SessionSlot) Replace(session *Session) {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		if session != nil {
			_ = session.Close()
		}
		return
	}
	old := s.session
	s.session = session
	onChange := s.onChange
	s.mu.Unlock()
	if old != nil && old != session {
		_ = old.Close()
	}
	if onChange != nil {
		onChange()
	}
}

func (s *SessionSlot) current() *Session {
	s.mu.RLock()
	session := s.session
	s.mu.RUnlock()
	return session
}

func (s *SessionSlot) setChangeHandler(onChange func()) {
	s.mu.Lock()
	s.onChange = onChange
	s.mu.Unlock()
}

func (s *SessionSlot) Close() {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return
	}
	s.closed = true
	session := s.session
	s.session = nil
	s.mu.Unlock()
	if session != nil {
		_ = session.Close()
	}
}

func NewPool(sessions ...*Session) *Pool {
	slots := make([]*SessionSlot, 0, len(sessions))
	for _, session := range sessions {
		if session != nil {
			slot := NewSessionSlot()
			slot.Replace(session)
			slots = append(slots, slot)
		}
	}
	return NewPoolSlots(slots...)
}

func NewPoolSlots(slots ...*SessionSlot) *Pool {
	active := make([]*SessionSlot, 0, len(slots))
	for _, slot := range slots {
		if slot != nil {
			active = append(active, slot)
		}
	}
	p := &Pool{
		slots:   active,
		done:    make(chan struct{}),
		changed: make(chan struct{}, 1),
	}
	for _, slot := range active {
		slot.setChangeHandler(p.signalChanged)
	}
	return p
}

func (p *Pool) Done() <-chan struct{} { return p.done }

func (p *Pool) signalDone() {
	p.doneOnce.Do(func() { close(p.done) })
}

func (p *Pool) signalChanged() {
	select {
	case p.changed <- struct{}{}:
	default:
	}
}

func (p *Pool) Open(ctx context.Context, address string) (*Stream, error) {
	return p.open(ctx, address, false)
}

func (p *Pool) OpenDatagram(ctx context.Context, address string) (*Stream, error) {
	return p.open(ctx, address, true)
}

func (p *Pool) open(ctx context.Context, address string, datagram bool) (*Stream, error) {
	if len(p.slots) == 0 {
		return nil, errors.New("proxy session pool is empty")
	}
	for {
		start := int(p.next.Add(1)-1) % len(p.slots)
		var lastErr error
		allClosed := true
		for offset := 0; offset < len(p.slots); offset++ {
			session := p.slots[(start+offset)%len(p.slots)].current()
			if session == nil {
				lastErr = ErrClosed
				continue
			}
			var stream *Stream
			var err error
			if datagram {
				stream, err = session.OpenDatagram(ctx, address)
			} else {
				stream, err = session.Open(ctx, address)
			}
			if err == nil {
				return stream, nil
			}
			lastErr = err
			if !errors.Is(err, ErrClosed) {
				allClosed = false
			}
		}
		if !allClosed {
			return nil, lastErr
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-p.done:
			return nil, ErrClosed
		case <-p.changed:
		}
	}
}

func (p *Pool) Close() error {
	p.closeOnce.Do(func() {
		p.signalDone()
		for _, slot := range p.slots {
			slot.Close()
		}
	})
	return nil
}
