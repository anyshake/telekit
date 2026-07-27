package streammux

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

const (
	frameOpen  byte = 1
	frameData  byte = 2
	frameClose byte = 3

	headerSize        = 13
	defaultMaxPayload = 32 << 10
	defaultBufferSize = 8 << 20
)

var ErrClosed = errors.New("stream mux closed")

type Session struct {
	conn       net.Conn
	maxPayload int
	bufferSize int

	nextID atomic.Uint64

	writeMu sync.Mutex

	mu      sync.RWMutex
	streams map[uint64]*Stream
	err     error

	acceptCh chan *Stream
	done     chan struct{}
	once     sync.Once
}

func NewClient(conn net.Conn) *Session {
	session := newSession(conn)
	session.nextID.Store(1)
	go session.readLoop()
	return session
}

func NewServer(conn net.Conn) *Session {
	session := newSession(conn)
	session.nextID.Store(2)
	go session.readLoop()
	return session
}

func newSession(conn net.Conn) *Session {
	return &Session{
		conn:       conn,
		maxPayload: defaultMaxPayload,
		bufferSize: defaultBufferSize,
		streams:    make(map[uint64]*Stream),
		acceptCh:   make(chan *Stream, 128),
		done:       make(chan struct{}),
	}
}

func (s *Session) Dial(ctx context.Context) (*Stream, error) {
	if ctx == nil {
		ctx = context.Background()
	}

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	stream := s.newStream(s.nextID.Add(2) - 2)
	if err := s.addStream(stream); err != nil {
		return nil, err
	}

	if err := s.writeFrame(frameOpen, stream.id, nil); err != nil {
		s.removeStream(stream.id)
		stream.abortLocal()
		return nil, err
	}

	select {
	case <-ctx.Done():
		_ = stream.Close()
		return nil, ctx.Err()
	default:
	}

	return stream, nil
}

func (s *Session) Accept() (*Stream, error) {
	select {
	case stream := <-s.acceptCh:
		return stream, nil
	case <-s.done:
		if err := s.closeErr(); err != nil {
			return nil, err
		}
		return nil, ErrClosed
	}
}

func (s *Session) Close() error {
	s.closeWithError(ErrClosed)
	return s.conn.Close()
}

func (s *Session) IsClosed() bool {
	select {
	case <-s.done:
		return true
	default:
		return false
	}
}

func (s *Session) newStream(id uint64) *Stream {
	return &Stream{
		id:      id,
		session: s,
		recv:    newRecvBuffer(s.bufferSize),
		done:    make(chan struct{}),
	}
}

func (s *Session) addStream(stream *Stream) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.err != nil {
		return s.err
	}
	s.streams[stream.id] = stream
	return nil
}

func (s *Session) removeStream(id uint64) {
	s.mu.Lock()
	delete(s.streams, id)
	s.mu.Unlock()
}

func (s *Session) getStream(id uint64) *Stream {
	s.mu.RLock()
	stream := s.streams[id]
	s.mu.RUnlock()
	return stream
}

func (s *Session) readLoop() {
	var header [headerSize]byte

	for {
		if _, err := io.ReadFull(s.conn, header[:]); err != nil {
			s.closeWithError(err)
			return
		}

		typ := header[0]
		id := binary.BigEndian.Uint64(header[1:9])
		length := binary.BigEndian.Uint32(header[9:13])
		if length > uint32(s.maxPayload) {
			s.closeWithError(fmt.Errorf("stream mux frame too large: %d", length))
			return
		}

		var payload []byte
		if length > 0 {
			payload = make([]byte, length)
			if _, err := io.ReadFull(s.conn, payload); err != nil {
				s.closeWithError(err)
				return
			}
		}

		switch typ {
		case frameOpen:
			stream := s.newStream(id)
			if err := s.addStream(stream); err != nil {
				stream.abortLocal()
				return
			}
			select {
			case s.acceptCh <- stream:
			case <-s.done:
				stream.abortLocal()
				return
			}

		case frameData:
			stream := s.getStream(id)
			if stream == nil {
				continue
			}
			if err := stream.recv.Write(payload); err != nil {
				_ = stream.Close()
				s.closeWithError(fmt.Errorf("stream %d receive buffer: %w", id, err))
				return
			}

		case frameClose:
			stream := s.getStream(id)
			if stream == nil {
				continue
			}
			s.removeStream(id)
			stream.closeRemote()

		default:
			s.closeWithError(fmt.Errorf("unknown stream mux frame type: %d", typ))
			return
		}
	}
}

func (s *Session) writeFrame(typ byte, id uint64, payload []byte) error {
	if len(payload) > s.maxPayload {
		return fmt.Errorf("stream mux payload too large: %d", len(payload))
	}

	select {
	case <-s.done:
		if err := s.closeErr(); err != nil {
			return err
		}
		return ErrClosed
	default:
	}

	var header [headerSize]byte
	header[0] = typ
	binary.BigEndian.PutUint64(header[1:9], id)
	binary.BigEndian.PutUint32(header[9:13], uint32(len(payload)))

	s.writeMu.Lock()
	defer s.writeMu.Unlock()

	if err := writeFull(s.conn, header[:]); err != nil {
		s.closeWithError(err)
		return err
	}
	if len(payload) == 0 {
		return nil
	}
	if err := writeFull(s.conn, payload); err != nil {
		s.closeWithError(err)
		return err
	}
	return nil
}

func writeFull(writer io.Writer, data []byte) error {
	for len(data) > 0 {
		n, err := writer.Write(data)
		if n > 0 {
			data = data[n:]
		}
		if err != nil {
			return err
		}
		if n == 0 {
			return io.ErrShortWrite
		}
	}
	return nil
}

func (s *Session) closeWithError(err error) {
	if err == nil {
		err = ErrClosed
	}

	s.once.Do(func() {
		s.mu.Lock()
		s.err = err
		streams := make([]*Stream, 0, len(s.streams))
		for _, stream := range s.streams {
			streams = append(streams, stream)
		}
		s.streams = make(map[uint64]*Stream)
		s.mu.Unlock()

		for _, stream := range streams {
			stream.abortLocal()
		}

		close(s.done)
	})
}

func (s *Session) closeErr() error {
	s.mu.RLock()
	err := s.err
	s.mu.RUnlock()
	return err
}

type Stream struct {
	id      uint64
	session *Session
	recv    *recvBuffer

	closeOnce sync.Once
	done      chan struct{}

	deadlineMu    sync.RWMutex
	writeDeadline time.Time
}

func (s *Stream) Read(p []byte) (int, error) {
	return s.recv.Read(p)
}

func (s *Stream) Write(p []byte) (int, error) {
	select {
	case <-s.done:
		return 0, io.ErrClosedPipe
	default:
	}

	if s.writeTimedOut() {
		return 0, os.ErrDeadlineExceeded
	}

	written := 0
	for written < len(p) {
		end := written + s.session.maxPayload
		if end > len(p) {
			end = len(p)
		}
		if err := s.session.writeFrame(frameData, s.id, p[written:end]); err != nil {
			return written, err
		}
		written = end
	}
	return written, nil
}

func (s *Stream) Close() error {
	var err error
	s.closeOnce.Do(func() {
		close(s.done)
		s.recv.abort()
		s.session.removeStream(s.id)
		err = s.session.writeFrame(frameClose, s.id, nil)
	})
	return err
}

func (s *Stream) LocalAddr() net.Addr {
	return streamAddr{network: "streammux", id: s.id}
}

func (s *Stream) RemoteAddr() net.Addr {
	return s.session.conn.RemoteAddr()
}

func (s *Stream) SetDeadline(deadline time.Time) error {
	s.recv.SetDeadline(deadline)
	return s.SetWriteDeadline(deadline)
}

func (s *Stream) SetReadDeadline(deadline time.Time) error {
	s.recv.SetDeadline(deadline)
	return nil
}

func (s *Stream) SetWriteDeadline(deadline time.Time) error {
	s.deadlineMu.Lock()
	s.writeDeadline = deadline
	s.deadlineMu.Unlock()
	return nil
}

func (s *Stream) closeRemote() {
	s.closeOnce.Do(func() {
		close(s.done)
		s.recv.closeEOF()
	})
}

func (s *Stream) abortLocal() {
	s.closeOnce.Do(func() {
		close(s.done)
		s.recv.abort()
	})
}

func (s *Stream) writeTimedOut() bool {
	s.deadlineMu.RLock()
	deadline := s.writeDeadline
	s.deadlineMu.RUnlock()
	return !deadline.IsZero() && time.Now().After(deadline)
}

type streamAddr struct {
	network string
	id      uint64
}

func (a streamAddr) Network() string { return a.network }
func (a streamAddr) String() string  { return fmt.Sprintf("%s:%d", a.network, a.id) }

type recvBuffer struct {
	cond     *sync.Cond
	buf      bytes.Buffer
	closed   bool
	deadline time.Time
	limit    int
}

func newRecvBuffer(limit int) *recvBuffer {
	if limit <= 0 {
		limit = defaultBufferSize
	}
	return &recvBuffer{
		cond:  sync.NewCond(&sync.Mutex{}),
		limit: limit,
	}
}

func (b *recvBuffer) Write(data []byte) error {
	b.cond.L.Lock()
	defer b.cond.L.Unlock()

	if b.closed {
		return io.ErrClosedPipe
	}
	if len(data) > b.limit-b.buf.Len() {
		return fmt.Errorf("receive buffer full")
	}
	if _, err := b.buf.Write(data); err != nil {
		return err
	}
	b.cond.Signal()
	return nil
}

func (b *recvBuffer) Read(p []byte) (int, error) {
	b.cond.L.Lock()
	defer b.cond.L.Unlock()

	for b.buf.Len() == 0 {
		if b.closed {
			return 0, io.EOF
		}
		if !b.deadline.IsZero() {
			remaining := time.Until(b.deadline)
			if remaining <= 0 {
				return 0, os.ErrDeadlineExceeded
			}
			timer := time.AfterFunc(remaining, func() {
				b.cond.L.Lock()
				b.cond.Broadcast()
				b.cond.L.Unlock()
			})
			b.cond.Wait()
			timer.Stop()
			continue
		}
		b.cond.Wait()
	}

	n, err := b.buf.Read(p)
	if b.buf.Len() == 0 {
		b.buf = bytes.Buffer{}
	}
	return n, err
}

func (b *recvBuffer) SetDeadline(deadline time.Time) {
	b.cond.L.Lock()
	b.deadline = deadline
	b.cond.Broadcast()
	b.cond.L.Unlock()
}

func (b *recvBuffer) closeEOF() {
	b.cond.L.Lock()
	b.closed = true
	b.cond.Broadcast()
	b.cond.L.Unlock()
}

func (b *recvBuffer) abort() {
	b.cond.L.Lock()
	b.closed = true
	b.buf = bytes.Buffer{}
	b.cond.Broadcast()
	b.cond.L.Unlock()
}
