package main

import (
	"crypto/ed25519"
	"encoding/base64"
	"fmt"
)

func decodeServerPublicKey(pubKey string) (ed25519.PublicKey, error) {
	key, err := base64.StdEncoding.DecodeString(pubKey)
	if err != nil {
		return nil, fmt.Errorf("decode server public key: %w", err)
	}

	if len(key) != ed25519.PublicKeySize {
		return nil, fmt.Errorf("server public key must be %d bytes", ed25519.PublicKeySize)
	}

	return ed25519.PublicKey(key), nil
}
