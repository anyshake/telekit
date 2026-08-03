package main

import (
	"crypto/ed25519"
	"encoding/hex"
	"fmt"
	"strings"
)

func decodeIdentityKey(value string) (ed25519.PrivateKey, error) {
	seed, err := hex.DecodeString(strings.TrimSpace(value))
	if err != nil {
		return nil, fmt.Errorf("decode identity seed: %w", err)
	}
	if len(seed) != ed25519.SeedSize {
		return nil, fmt.Errorf("identity seed must be %d bytes", ed25519.SeedSize)
	}
	return ed25519.NewKeyFromSeed(seed), nil
}
