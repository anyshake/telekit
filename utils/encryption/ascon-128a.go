package encryption

import (
	"crypto/cipher"
	"crypto/rand"
	"crypto/sha512"
	"errors"
	"io"

	"github.com/cloudflare/circl/cipher/ascon"
	"golang.org/x/crypto/hkdf"
)

type Ascon128aImpl struct {
	aead cipher.AEAD
}

func NewAscon128a(secret []byte) (IEncryption, error) {
	key := make([]byte, ascon.KeySize) // 16 bytes
	if _, err := hkdf.New(sha512.New, secret, nil, nil).Read(key); err != nil {
		return nil, err
	}

	aead, err := ascon.New(key, ascon.Ascon128a)
	if err != nil {
		return nil, err
	}

	return &Ascon128aImpl{aead: aead}, nil
}

func (impl *Ascon128aImpl) Encrypt(plaintext, aad []byte) ([]byte, error) {
	nonce := make([]byte, impl.aead.NonceSize()) // 16 bytes
	if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
		return nil, err
	}

	ciphertext := impl.aead.Seal(nil, nonce, plaintext, aad)
	return append(nonce, ciphertext...), nil
}

func (impl *Ascon128aImpl) Decrypt(data, aad []byte) ([]byte, error) {
	nonceSize := impl.aead.NonceSize()
	if len(data) < nonceSize {
		return nil, errors.New("ciphertext too short")
	}

	nonce, ciphertext := data[:nonceSize], data[nonceSize:]
	return impl.aead.Open(nil, nonce, ciphertext, aad)
}
