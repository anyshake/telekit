package protocol

import (
	"errors"
	"net"
	"sync"
	"sync/atomic"
)

const (
	frameOpen byte = iota + 1
	frameOpenDatagram
	frameOpenOK
	frameOpenError
	frameData
	frameEOF
	frameClose

	frameHeaderSize = 9
	maxFramePayload = 64 << 10
	maxTargetLength = 4 << 10
	maxStreamQueue  = 4 << 20
)

var ErrClosed = net.ErrClosed

// ErrUnavailable indicates that a session's underlying transport is being
// re-established. Callers may retry the operation on another session or wait
// for this session to become usable again.
var ErrUnavailable = errors.New("proxy session unavailable")

type Request struct {
	Address  string
	Stream   *Stream
	Datagram bool
}

type Session struct {
	conn net.Conn

	writeMu   sync.Mutex
	streamsMu sync.Mutex
	streams   map[uint32]*Stream
	nextID    atomic.Uint32

	requests  chan *Request
	done      chan struct{}
	closeOnce sync.Once
}
