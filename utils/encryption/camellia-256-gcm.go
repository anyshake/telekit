package encryption

import (
	"crypto/cipher"
	"crypto/rand"
	"crypto/sha512"
	"errors"
	"io"

	"github.com/aead/camellia"
	"golang.org/x/crypto/hkdf"
)

type CamelliaGCMImpl struct {
	aead cipher.AEAD
}

func NewCamellia256GCM(secret []byte) (IEncryption, error) {
	key := make([]byte, 32) // Camellia-256
	if _, err := hkdf.New(sha512.New, secret, nil, nil).Read(key); err != nil {
		return nil, err
	}

	block, err := camellia.NewCipher(key)
	if err != nil {
		return nil, err
	}

	aead, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}

	return &CamelliaGCMImpl{aead: aead}, nil
}

func (impl *CamelliaGCMImpl) Encrypt(plaintext, aad []byte) ([]byte, error) {
	nonce := make([]byte, impl.aead.NonceSize()) // 12 bytes
	if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
		return nil, err
	}

	ciphertext := impl.aead.Seal(nil, nonce, plaintext, aad)
	return append(nonce, ciphertext...), nil
}

func (impl *CamelliaGCMImpl) Decrypt(data, aad []byte) ([]byte, error) {
	nonceSize := impl.aead.NonceSize()
	if len(data) < nonceSize {
		return nil, errors.New("ciphertext too short")
	}

	nonce, ciphertext := data[:nonceSize], data[nonceSize:]
	return impl.aead.Open(nil, nonce, ciphertext, aad)
}
