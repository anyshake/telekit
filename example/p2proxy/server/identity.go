package main

import (
	"crypto/ed25519"
	"encoding/hex"
	"fmt"
)

func decodeIdentityKey(value string) (ed25519.PrivateKey, error) {
	seed, err := hex.DecodeString(value)
	if err != nil || len(seed) != ed25519.SeedSize {
		return nil, fmt.Errorf("identity seed must be %d-byte hex", ed25519.SeedSize)
	}
	return ed25519.NewKeyFromSeed(seed), nil
}
