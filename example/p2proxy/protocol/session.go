package protocol

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
)

func NewSession(conn net.Conn) *Session {
	s := &Session{
		conn:     conn,
		streams:  make(map[uint32]*Stream),
		requests: make(chan *Request, 64),
		done:     make(chan struct{}),
	}
	go s.readLoop()
	return s
}

func (s *Session) Done() <-chan struct{} { return s.done }

func (s *Session) Close() error {
	var err error
	s.closeOnce.Do(func() {
		close(s.done)
		err = s.conn.Close()
		s.streamsMu.Lock()
		streams := make([]*Stream, 0, len(s.streams))
		for _, stream := range s.streams {
			streams = append(streams, stream)
		}
		s.streams = make(map[uint32]*Stream)
		s.streamsMu.Unlock()
		for _, stream := range streams {
			stream.markClosed()
		}
	})
	return err
}

func (s *Session) Open(ctx context.Context, address string) (*Stream, error) {
	if len(address) == 0 || len(address) > maxTargetLength {
		return nil, errors.New("proxy target is invalid")
	}
	select {
	case <-s.done:
		return nil, ErrClosed
	default:
	}

	id := s.nextID.Add(1)
	stream := newStream(s, id)
	stream.openResult = make(chan error, 1)
	s.addStream(stream)
	if err := s.send(frameOpen, id, []byte(address)); err != nil {
		stream.failOpen(err)
		return nil, err
	}
	select {
	case err := <-stream.openResult:
		if err != nil {
			stream.failOpen(err)
			return nil, err
		}
		return stream, nil
	case <-ctx.Done():
		stream.failOpen(ctx.Err())
		return nil, ctx.Err()
	case <-s.done:
		stream.failOpen(ErrClosed)
		return nil, ErrClosed
	}
}

func (s *Session) Accept(ctx context.Context) (*Request, error) {
	select {
	case request := <-s.requests:
		if request == nil {
			return nil, ErrClosed
		}
		return request, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-s.done:
		return nil, ErrClosed
	}
}

func (s *Session) addStream(stream *Stream) {
	s.streamsMu.Lock()
	s.streams[stream.id] = stream
	s.streamsMu.Unlock()
}

func (s *Session) getStream(id uint32) *Stream {
	s.streamsMu.Lock()
	stream := s.streams[id]
	s.streamsMu.Unlock()
	return stream
}

func (s *Session) removeStream(id uint32) {
	s.streamsMu.Lock()
	delete(s.streams, id)
	s.streamsMu.Unlock()
}

func (s *Session) send(kind byte, id uint32, payload []byte) error {
	if len(payload) > maxFramePayload {
		return errors.New("proxy frame is too large")
	}
	select {
	case <-s.done:
		return ErrClosed
	default:
	}
	var header [frameHeaderSize]byte
	header[0] = kind
	binary.BigEndian.PutUint32(header[1:5], id)
	binary.BigEndian.PutUint32(header[5:9], uint32(len(payload)))
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	if err := s.writeFull(s.conn, header[:]); err != nil {
		return err
	}
	if len(payload) != 0 {
		return s.writeFull(s.conn, payload)
	}
	return nil
}

func (s *Session) writeFull(writer io.Writer, data []byte) error {
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

func (s *Session) readLoop() {
	var header [frameHeaderSize]byte
	for {
		if _, err := io.ReadFull(s.conn, header[:]); err != nil {
			_ = s.Close()
			return
		}
		kind := header[0]
		id := binary.BigEndian.Uint32(header[1:5])
		length := binary.BigEndian.Uint32(header[5:9])
		if length > maxFramePayload {
			_ = s.Close()
			return
		}
		payload := make([]byte, length)
		if _, err := io.ReadFull(s.conn, payload); err != nil {
			_ = s.Close()
			return
		}
		s.handleFrame(kind, id, payload)
	}
}

func (s *Session) handleFrame(kind byte, id uint32, payload []byte) {
	switch kind {
	case frameOpen:
		if len(payload) == 0 || len(payload) > maxTargetLength {
			_ = s.send(frameOpenError, id, []byte("invalid proxy target"))
			return
		}
		if s.getStream(id) != nil {
			_ = s.send(frameOpenError, id, []byte("duplicate proxy stream"))
			return
		}
		stream := newStream(s, id)
		s.addStream(stream)
		request := &Request{Address: string(payload), Stream: stream}
		select {
		case s.requests <- request:
		case <-s.done:
			stream.markClosed()
		}
	case frameOpenOK:
		if stream := s.getStream(id); stream != nil && stream.openResult != nil {
			select {
			case stream.openResult <- nil:
			default:
			}
		}
	case frameOpenError:
		if stream := s.getStream(id); stream != nil && stream.openResult != nil {
			err := fmt.Errorf("remote target connection failed: %s", string(payload))
			select {
			case stream.openResult <- err:
			default:
			}
		}
	case frameData:
		if stream := s.getStream(id); stream != nil {
			if !stream.enqueue(payload) {
				stream.markClosed()
				_ = s.send(frameClose, id, nil)
			}
		}
	case frameClose:
		if stream := s.getStream(id); stream != nil {
			stream.markClosed()
		}
	case frameEOF:
		if stream := s.getStream(id); stream != nil {
			stream.markRemoteEOF()
		}
	}
}
