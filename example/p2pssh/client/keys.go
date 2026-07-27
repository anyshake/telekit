package main

import (
	"crypto/ed25519"
	"encoding/hex"
	"fmt"
	"strings"
)

func decodeServerPublicKey(
	value string,
) (ed25519.PublicKey, error) {
	value = strings.TrimSpace(value)

	key, err := hex.DecodeString(value)
	if err != nil {
		return nil, fmt.Errorf(
			"decode server public key: %w",
			err,
		)
	}

	if len(key) != ed25519.PublicKeySize {
		return nil, fmt.Errorf(
			"server public key must be %d bytes, got %d",
			ed25519.PublicKeySize,
			len(key),
		)
	}

	return ed25519.PublicKey(key), nil
}
