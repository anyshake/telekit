package encryption

import (
	"crypto/rand"
	"encoding/binary"
	"errors"
	"io"
	"math"
	"sync/atomic"
)

const nonceCounterSize = 8

// nonceSource avoids a crypto/rand syscall for every encrypted frame. A fresh
// random prefix is generated once per encrypting key instance, and the counter
// makes every nonce unique for that instance's lifetime.
type nonceSource struct {
	prefix  []byte
	counter atomic.Uint64
}

func newNonceSource(size int) (*nonceSource, error) {
	if size <= nonceCounterSize {
		return nil, errors.New("nonce size is too small")
	}
	prefix := make([]byte, size-nonceCounterSize)
	if _, err := io.ReadFull(rand.Reader, prefix); err != nil {
		return nil, err
	}
	return &nonceSource{prefix: prefix}, nil
}

func (s *nonceSource) next(dst []byte) error {
	if len(dst) != len(s.prefix)+nonceCounterSize {
		return errors.New("invalid nonce size")
	}
	var counter uint64
	for {
		current := s.counter.Load()
		if current == math.MaxUint64 {
			return errors.New("nonce counter exhausted")
		}
		counter = current + 1
		if s.counter.CompareAndSwap(current, counter) {
			break
		}
	}
	copy(dst, s.prefix)
	binary.BigEndian.PutUint64(dst[len(s.prefix):], counter)
	return nil
}
