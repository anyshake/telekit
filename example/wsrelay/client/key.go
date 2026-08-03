package main

import (
	"crypto/ed25519"
	"encoding/hex"
	"fmt"
	"strings"
)

func decodeServerPublicKey(value string) (ed25519.PublicKey, error) {
	key, err := hex.DecodeString(strings.TrimSpace(value))
	if err != nil {
		return nil, fmt.Errorf("decode server public key: %w", err)
	}
	return ed25519.PublicKey(key), nil
}
