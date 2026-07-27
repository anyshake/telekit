package main

import (
	"crypto/ed25519"
	"encoding/hex"
	"fmt"
)

const defaultIdentitySeed = "9d61b19deffd5a60ba844af492ec2cc44449c5697b326919703bac031cae7f60"

func decodeIdentityKey(value string) (ed25519.PrivateKey, error) {
	seed, err := hex.DecodeString(value)
	if err != nil || len(seed) != ed25519.SeedSize {
		return nil, fmt.Errorf("identity seed must be %d-byte hex", ed25519.SeedSize)
	}
	return ed25519.NewKeyFromSeed(seed), nil
}
