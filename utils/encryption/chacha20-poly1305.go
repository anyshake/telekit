package encryption

import (
	"crypto/cipher"
	"crypto/sha512"
	"errors"

	"golang.org/x/crypto/chacha20poly1305"
	"golang.org/x/crypto/hkdf"
)

type ChaCha20Poly1305Impl struct {
	aead  cipher.AEAD
	nonce *nonceSource
}

func NewChaCha20Poly1305(secret []byte) (IEncryption, error) {
	key := make([]byte, chacha20poly1305.KeySize)
	_, err := hkdf.New(sha512.New, secret, nil, nil).Read(key)

	aead, err := chacha20poly1305.New(key)
	if err != nil {
		return nil, err
	}
	nonce, err := newNonceSource(aead.NonceSize())
	if err != nil {
		return nil, err
	}

	return &ChaCha20Poly1305Impl{aead: aead, nonce: nonce}, nil
}

func (impl *ChaCha20Poly1305Impl) Encrypt(plaintext, aad []byte) ([]byte, error) {
	nonce := make([]byte, impl.aead.NonceSize())
	if err := impl.nonce.next(nonce); err != nil {
		return nil, err
	}

	result := make([]byte, len(nonce), len(nonce)+len(plaintext)+impl.aead.Overhead())
	copy(result, nonce)
	return impl.aead.Seal(result, nonce, plaintext, aad), nil
}

func (impl *ChaCha20Poly1305Impl) Decrypt(data, aad []byte) ([]byte, error) {
	nonceSize := impl.aead.NonceSize()
	if len(data) < nonceSize {
		return nil, errors.New("ciphertext too short")
	}

	nonce, ciphertext := data[:nonceSize], data[nonceSize:]
	return impl.aead.Open(nil, nonce, ciphertext, aad)
}
