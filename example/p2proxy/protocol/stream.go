package protocol

import (
	"errors"
	"io"
	"sync"
)

type Stream struct {
	session *Session
	id      uint32

	incomingMu     sync.Mutex
	incomingQueue  [][]byte
	incomingBytes  int
	incomingNotify chan struct{}
	closed         chan struct{}
	remoteEOF      chan struct{}
	writeEOF       chan struct{}
	closeOnce      sync.Once
	readMu         sync.Mutex
	writeMu        sync.Mutex

	openResult chan error
	responseMu sync.Mutex
	responded  bool
	writeOnce  sync.Once
}

func newStream(session *Session, id uint32) *Stream {
	return &Stream{
		session:        session,
		id:             id,
		incomingNotify: make(chan struct{}, 1),
		closed:         make(chan struct{}),
		remoteEOF:      make(chan struct{}),
		writeEOF:       make(chan struct{}),
	}
}

func (s *Stream) enqueue(data []byte) bool {
	if len(data) == 0 {
		return true
	}
	s.incomingMu.Lock()
	defer s.incomingMu.Unlock()
	select {
	case <-s.closed:
		return false
	default:
	}
	if s.incomingBytes+len(data) > maxStreamQueue {
		return false
	}
	s.incomingQueue = append(s.incomingQueue, data)
	s.incomingBytes += len(data)
	select {
	case s.incomingNotify <- struct{}{}:
	default:
	}
	return true
}

func (s *Stream) dequeue(p []byte) int {
	s.incomingMu.Lock()
	defer s.incomingMu.Unlock()
	if len(s.incomingQueue) == 0 {
		return 0
	}
	data := s.incomingQueue[0]
	n := copy(p, data)
	if n == len(data) {
		s.incomingQueue[0] = nil
		s.incomingQueue = s.incomingQueue[1:]
	} else {
		s.incomingQueue[0] = data[n:]
	}
	s.incomingBytes -= n
	return n
}

func (s *Stream) Write(p []byte) (int, error) {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	for offset := 0; offset < len(p); {
		select {
		case <-s.closed:
			return offset, io.ErrClosedPipe
		case <-s.writeEOF:
			return offset, io.ErrClosedPipe
		default:
		}
		end := offset + maxFramePayload
		if end > len(p) {
			end = len(p)
		}
		if err := s.session.send(frameData, s.id, p[offset:end]); err != nil {
			return offset, err
		}
		offset = end
	}
	return len(p), nil
}

// CloseWrite signals EOF in this stream's write direction while leaving the
// read direction open for the peer's response.
func (s *Stream) CloseWrite() error {
	var err error
	s.writeMu.Lock()
	select {
	case <-s.closed:
		s.writeMu.Unlock()
		return io.ErrClosedPipe
	default:
	}
	s.writeOnce.Do(func() {
		close(s.writeEOF)
		err = s.session.send(frameEOF, s.id, nil)
	})
	s.writeMu.Unlock()
	return err
}

func (s *Stream) Read(p []byte) (int, error) {
	s.readMu.Lock()
	defer s.readMu.Unlock()
	for {
		if n := s.dequeue(p); n > 0 {
			return n, nil
		}
		select {
		case <-s.incomingNotify:
			continue
		case <-s.closed:
			if n := s.dequeue(p); n > 0 {
				return n, nil
			}
			return 0, io.EOF
		case <-s.remoteEOF:
			if n := s.dequeue(p); n > 0 {
				return n, nil
			}
			return 0, io.EOF
		case <-s.session.done:
			if n := s.dequeue(p); n > 0 {
				return n, nil
			}
			return 0, io.EOF
		}
	}
}

func (s *Stream) Accept() error {
	return s.respond(frameOpenOK, nil)
}

func (s *Stream) Reject(err error) error {
	message := []byte("connection rejected")
	if err != nil && err.Error() != "" {
		message = []byte(err.Error())
	}
	if len(message) > maxFramePayload {
		message = message[:maxFramePayload]
	}
	return s.respond(frameOpenError, message)
}

func (s *Stream) respond(kind byte, payload []byte) error {
	s.responseMu.Lock()
	if s.responded {
		s.responseMu.Unlock()
		return errors.New("proxy stream already acknowledged")
	}
	s.responded = true
	s.responseMu.Unlock()
	return s.session.send(kind, s.id, payload)
}

func (s *Stream) Close() error {
	var err error
	s.closeOnce.Do(func() {
		s.writeMu.Lock()
		select {
		case <-s.writeEOF:
		default:
			close(s.writeEOF)
		}
		close(s.closed)
		s.session.removeStream(s.id)
		err = s.session.send(frameClose, s.id, nil)
		s.writeMu.Unlock()
	})
	return err
}

func (s *Stream) failOpen(err error) {
	s.closeOnce.Do(func() {
		close(s.closed)
		s.session.removeStream(s.id)
	})
	if s.openResult != nil {
		select {
		case s.openResult <- err:
		default:
		}
	}
	_ = s.session.send(frameClose, s.id, nil)
}

func (s *Stream) markClosed() {
	s.closeOnce.Do(func() {
		close(s.closed)
		s.session.removeStream(s.id)
	})
}

func (s *Stream) markRemoteEOF() {
	select {
	case <-s.remoteEOF:
	default:
		close(s.remoteEOF)
	}
}
