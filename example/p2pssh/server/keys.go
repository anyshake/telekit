package main

import (
	"crypto/ed25519"
	"crypto/rand"
	"encoding/hex"
	"encoding/pem"
	"errors"
	"fmt"
	"log"
	"os"
	"strings"

	"golang.org/x/crypto/ssh"
)

func loadOrCreateHostKey(path string) (ssh.Signer, error) {
	keyData, err := os.ReadFile(path)
	if err == nil {
		signer, parseErr := ssh.ParsePrivateKey(keyData)
		if parseErr != nil {
			return nil, fmt.Errorf(
				"parse SSH host key %q: %w",
				path,
				parseErr,
			)
		}

		return signer, nil
	}

	if !errors.Is(err, os.ErrNotExist) {
		return nil, fmt.Errorf(
			"read SSH host key %q: %w",
			path,
			err,
		)
	}

	_, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		return nil, fmt.Errorf(
			"generate SSH host key: %w",
			err,
		)
	}

	pkcs8, err := ssh.MarshalPrivateKey(privateKey, "")
	if err != nil {
		return nil, fmt.Errorf(
			"marshal SSH host key: %w",
			err,
		)
	}

	pemBytes := pem.EncodeToMemory(pkcs8)
	if pemBytes == nil {
		return nil, errors.New("encode SSH host key as PEM failed")
	}

	if err := os.WriteFile(path, pemBytes, 0600); err != nil {
		return nil, fmt.Errorf(
			"write SSH host key %q: %w",
			path,
			err,
		)
	}

	signer, err := ssh.ParsePrivateKey(pemBytes)
	if err != nil {
		return nil, fmt.Errorf(
			"parse generated SSH host key: %w",
			err,
		)
	}

	return signer, nil
}

func decodeIdentityKey(value string) (ed25519.PrivateKey, error) {
	value = strings.TrimSpace(value)

	seed, err := hex.DecodeString(value)
	if err != nil {
		return nil, fmt.Errorf(
			"decode identity seed: %w",
			err,
		)
	}

	if len(seed) != ed25519.SeedSize {
		return nil, fmt.Errorf(
			"identity seed must be %d bytes, got %d",
			ed25519.SeedSize,
			len(seed),
		)
	}

	return ed25519.NewKeyFromSeed(seed), nil
}

func setupKeyProvider(key []byte) func(clientID string) ([]byte, error) {
	return func(clientID string) ([]byte, error) {
		log.Printf(
			"handshake: authenticating client=%q",
			clientID,
		)

		return append([]byte(nil), key[:]...), nil
	}
}
