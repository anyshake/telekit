package protocol

import (
	"errors"
	"io"
	"sync"
)

type Stream struct {
	session *Session
	id      uint32

	incoming  chan []byte
	closed    chan struct{}
	remoteEOF chan struct{}
	writeEOF  chan struct{}
	closeOnce sync.Once
	readMu    sync.Mutex
	writeMu   sync.Mutex
	pending   []byte

	openResult chan error
	responseMu sync.Mutex
	responded  bool
	writeOnce  sync.Once
}

func newStream(session *Session, id uint32) *Stream {
	return &Stream{
		session:   session,
		id:        id,
		incoming:  make(chan []byte, 32),
		closed:    make(chan struct{}),
		remoteEOF: make(chan struct{}),
		writeEOF:  make(chan struct{}),
	}
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
	if len(s.pending) > 0 {
		n := copy(p, s.pending)
		s.pending = s.pending[n:]
		return n, nil
	}
	for {
		select {
		case data := <-s.incoming:
			if len(data) == 0 {
				continue
			}
			n := copy(p, data)
			if n < len(data) {
				s.pending = append(s.pending[:0], data[n:]...)
			}
			return n, nil
		case <-s.closed:
			if len(s.incoming) != 0 {
				continue
			}
			return 0, io.EOF
		case <-s.remoteEOF:
			if len(s.incoming) != 0 {
				continue
			}
			return 0, io.EOF
		case <-s.session.done:
			if len(s.incoming) != 0 {
				continue
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
