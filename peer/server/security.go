package server

import (
	"crypto/rand"
	"io"
	"time"

	"github.com/anyshake/telekit/peer"
)

func (s *Server) isNonceAvailable(nonce []byte) bool {
	str := string(nonce)

	if t, ok := s.nonceCache.Get(str); ok {
		timeObj := time.UnixMilli(t)
		if time.Since(timeObj) < s.options.ReplayProtection {
			return false
		}
	}

	s.nonceCache.Add(str, time.Now().UnixMilli())
	return true
}

func createRandomHandshakeValue() ([]byte, error) {
	value := make([]byte, peer.HandshakeNonceSize)
	if _, err := io.ReadFull(rand.Reader, value); err != nil {
		return nil, err
	}
	return value, nil
}

func (s *Server) isClientIdValid(clientId string) bool {
	return peer.ValidateClientID(clientId) == nil
}
