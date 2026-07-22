package encryption

import (
	"crypto/cipher"
	"crypto/rand"
	"crypto/sha512"
	"errors"
	"io"

	"golang.org/x/crypto/chacha20poly1305"
	"golang.org/x/crypto/hkdf"
)

type ChaCha20Poly1305Impl struct {
	aead cipher.AEAD
}

func NewChaCha20Poly1305(secret []byte) (IEncryption, error) {
	key := make([]byte, chacha20poly1305.KeySize)
	_, err := hkdf.New(sha512.New, secret, nil, nil).Read(key)

	aead, err := chacha20poly1305.New(key)
	if err != nil {
		return nil, err
	}

	return &ChaCha20Poly1305Impl{aead: aead}, nil
}

func (impl *ChaCha20Poly1305Impl) Encrypt(plaintext, aad []byte) ([]byte, error) {
	nonce := make([]byte, impl.aead.NonceSize())
	if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
		return nil, err
	}

	ciphertext := impl.aead.Seal(nil, nonce, plaintext, aad)
	return append(nonce, ciphertext...), nil
}

func (impl *ChaCha20Poly1305Impl) Decrypt(data, aad []byte) ([]byte, error) {
	nonceSize := impl.aead.NonceSize()
	if len(data) < nonceSize {
		return nil, errors.New("ciphertext too short")
	}

	nonce, ciphertext := data[:nonceSize], data[nonceSize:]
	return impl.aead.Open(nil, nonce, ciphertext, aad)
}
