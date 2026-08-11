package encryption

import (
	"crypto/cipher"
	"crypto/sha512"
	"errors"

	"golang.org/x/crypto/chacha20poly1305"
	"golang.org/x/crypto/hkdf"
)

type XChaCha20Poly1305Impl struct {
	aead  cipher.AEAD
	nonce *nonceSource
}

func NewXChaCha20Poly1305(secret []byte) (IEncryption, error) {
	key := make([]byte, chacha20poly1305.KeySize)
	if _, err := hkdf.New(sha512.New, secret, nil, nil).Read(key); err != nil {
		return nil, err
	}

	aead, err := chacha20poly1305.NewX(key)
	if err != nil {
		return nil, err
	}
	nonce, err := newNonceSource(aead.NonceSize())
	if err != nil {
		return nil, err
	}

	return &XChaCha20Poly1305Impl{aead: aead, nonce: nonce}, nil
}

func (impl *XChaCha20Poly1305Impl) Encrypt(plaintext, aad []byte) ([]byte, error) {
	nonce := make([]byte, impl.aead.NonceSize()) // 24 bytes
	if err := impl.nonce.next(nonce); err != nil {
		return nil, err
	}

	result := make([]byte, len(nonce), len(nonce)+len(plaintext)+impl.aead.Overhead())
	copy(result, nonce)
	return impl.aead.Seal(result, nonce, plaintext, aad), nil
}

func (impl *XChaCha20Poly1305Impl) Decrypt(data, aad []byte) ([]byte, error) {
	nonceSize := impl.aead.NonceSize()
	if len(data) < nonceSize {
		return nil, errors.New("ciphertext too short")
	}

	nonce, ciphertext := data[:nonceSize], data[nonceSize:]
	return impl.aead.Open(nil, nonce, ciphertext, aad)
}
