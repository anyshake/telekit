package encryption

const (
	AES_256_GCM        = "aes-256-gcm"
	ASCON_128A         = "ascon-128a"
	CAMELLIA_256       = "camellia-256-gcm"
	CHACHA20_POLY1305  = "chacha20-poly1305"
	XCHACHA20_POLY1305 = "xchacha20-poly1305"
)

type IEncryption interface {
	Encrypt(plaintext, aad []byte) ([]byte, error)
	Decrypt(data, aad []byte) ([]byte, error)
}
