package encryption

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"crypto/sha512"
	"errors"
	"io"

	"golang.org/x/crypto/hkdf"
)

type AES256GCMimpl struct {
	gcm cipher.AEAD
}

func NewAES256GCM(secret []byte) (IEncryption, error) {
	key := make([]byte, 32)
	_, err := hkdf.New(sha512.New, secret, nil, nil).Read(key)

	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}

	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}

	return &AES256GCMimpl{gcm: gcm}, nil
}

func (impl *AES256GCMimpl) Encrypt(plaintext, aad []byte) ([]byte, error) {
	nonce := make([]byte, impl.gcm.NonceSize())
	if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
		return nil, err
	}

	ciphertext := impl.gcm.Seal(nil, nonce, plaintext, aad)
	return append(nonce, ciphertext...), nil
}

func (impl *AES256GCMimpl) Decrypt(data, aad []byte) ([]byte, error) {
	nonceSize := impl.gcm.NonceSize()
	if len(data) < nonceSize {
		return nil, errors.New("ciphertext too short")
	}

	nonce, ciphertext := data[:nonceSize], data[nonceSize:]
	return impl.gcm.Open(nil, nonce, ciphertext, aad)
}
