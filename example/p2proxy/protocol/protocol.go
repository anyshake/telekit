package protocol

import (
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
