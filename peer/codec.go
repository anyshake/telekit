package peer

import (
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"fmt"
	"sync"

	"github.com/anyshake/telekit/utils/compression"
	"github.com/anyshake/telekit/utils/encryption"
	"github.com/samber/lo"
)

const (
	ENCRYPT_PARTIALY = 0x00
	ENCRYPT_GLOBALLY = 0x01
)

const (
	INDEX_HEADER = iota
	INDEX_PAYLOAD
	INDEX_ENCRYPT_FLAG
	INDEX_PACKET_LENGTH
)

const maxSignalingPacketSize = 16 << 20

const (
	aadDomainData byte = iota + 1
	aadDomainHeader
	aadDomainPayload
)

type Codec struct {
	mu     sync.Mutex
	secret []byte
	typ    string

	encryptionKit  encryption.IEncryption
	additionalData []byte
	useCompress    bool
}

func createEncryptKit(encryptionType string, secret []byte) (encryption.IEncryption, error) {
	switch encryptionType {
	case encryption.AES_256_GCM:
		return encryption.NewAES256GCM(secret)
	case encryption.ASCON_128A:
		return encryption.NewAscon128a(secret)
	case encryption.CAMELLIA_256:
		return encryption.NewCamellia256GCM(secret)
	case encryption.CHACHA20_POLY1305:
		return encryption.NewChaCha20Poly1305(secret)
	case encryption.XCHACHA20_POLY1305:
		return encryption.NewXChaCha20Poly1305(secret)
	default:
		return nil, fmt.Errorf("unknown encryption type: %s", encryptionType)
	}
}

func NewCodec(encryptionType string, secret, additionalData []byte, useCompress bool) (*Codec, error) {
	if len(additionalData) > 64 {
		return nil, fmt.Errorf("additional data is too long: %d, maximum is 64", len(additionalData))
	}

	encryptionKit, err := createEncryptKit(encryptionType, secret)
	if err != nil {
		return nil, err
	}

	return &Codec{
		encryptionKit:  encryptionKit,
		additionalData: append([]byte(nil), additionalData...),
		useCompress:    useCompress,
		secret:         append([]byte(nil), secret...),
		typ:            encryptionType,
	}, nil
}

func (e *Codec) UpdateSecret(encryptionType string, secret []byte) error {
	encryptionKit, err := createEncryptKit(encryptionType, secret)
	if err != nil {
		return err
	}

	e.mu.Lock()
	defer e.mu.Unlock()

	e.encryptionKit = encryptionKit
	e.secret = append([]byte(nil), secret...)
	e.typ = encryptionType
	return nil
}

func (e *Codec) GetSecret() (typ string, secret []byte) {
	e.mu.Lock()
	defer e.mu.Unlock()

	return e.typ, append([]byte(nil), e.secret...)
}

func (e *Codec) EncodeWithEncryption(data []byte) ([]byte, error) {
	return e.encodeWithEncryption(data, aadDomainData, nil)
}

func (e *Codec) encodeWithEncryption(data []byte, domain byte, context []byte) ([]byte, error) {
	if len(data) == 0 {
		return nil, fmt.Errorf("message data is empty")
	}

	e.mu.Lock()
	defer e.mu.Unlock()
	if e.encryptionKit == nil {
		return nil, fmt.Errorf("encryption kit is not set")
	}

	dataBytes := data
	var err error

	if e.useCompress {
		dataBytes, err = compression.Compress(dataBytes)
		if err != nil {
			return nil, err
		}
	}

	finalAAD := e.buildAdditionalData(e.additionalData, e.useCompress, domain, context)
	dataBytes, err = e.encryptionKit.Encrypt(dataBytes, finalAAD)
	if err != nil {
		return nil, err
	}

	return dataBytes, nil
}

func (e *Codec) EncodeMessage(m *Message) ([]byte, error) {
	if m == nil {
		return nil, fmt.Errorf("message is nil")
	}

	if m.Header == nil {
		m.Header = &Header{}
	}
	if m.Payload == nil {
		m.Payload = &Payload{}
	}

	plainHeaderBytes := m.Header.Marshal()
	headerBytes := plainHeaderBytes
	if m.Encrypt {
		var err error
		headerBytes, err = e.encodeWithEncryption(headerBytes, aadDomainHeader, nil)
		if err != nil {
			return nil, err
		}
	}

	payloadBytes, err := e.encodeWithEncryption(m.Payload.Marshal(), aadDomainPayload, plainHeaderBytes)
	if err != nil {
		return nil, err
	}

	dataChunks := [][]byte{
		headerBytes,  // Encrypt when m.Encrypt is true
		payloadBytes, // always Encrypt
		{lo.Ternary(m.Encrypt, byte(ENCRYPT_GLOBALLY), byte(ENCRYPT_PARTIALY))},
	}

	// to gob encoding
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	if err = enc.Encode(dataChunks); err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}

func (e *Codec) DecodeWithDecryption(data []byte) ([]byte, error) {
	return e.decodeWithDecryption(data, aadDomainData, nil, compression.MaxDecodedSize)
}

func (e *Codec) DecodeWithDecryptionLimit(data []byte, maxDecodedSize int) ([]byte, error) {
	return e.decodeWithDecryption(data, aadDomainData, nil, maxDecodedSize)
}

func (e *Codec) decodeWithDecryption(data []byte, domain byte, context []byte, maxDecodedSize int) ([]byte, error) {
	if len(data) == 0 {
		return nil, fmt.Errorf("data is empty")
	}

	e.mu.Lock()
	defer e.mu.Unlock()
	if e.encryptionKit == nil {
		return nil, fmt.Errorf("encryption kit is not set")
	}

	finalAAD := e.buildAdditionalData(e.additionalData, e.useCompress, domain, context)
	dataBytes, err := e.encryptionKit.Decrypt(data, finalAAD)
	if err != nil {
		return nil, err
	}
	if e.useCompress {
		dataBytes, err = compression.DecompressLimit(dataBytes, maxDecodedSize)
		if err != nil {
			return nil, err
		}
	}

	return dataBytes, nil
}

func (e *Codec) DecodeMessageHeader(data []byte) (*Header, error) {
	if len(data) == 0 {
		return nil, fmt.Errorf("data is empty")
	}
	if len(data) > maxSignalingPacketSize {
		return nil, fmt.Errorf("signaling packet exceeds %d bytes", maxSignalingPacketSize)
	}

	dec := gob.NewDecoder(bytes.NewBuffer(data))

	var dataChunks [][]byte
	if err := dec.Decode(&dataChunks); err != nil {
		return nil, err
	}
	if len(dataChunks) != INDEX_PACKET_LENGTH {
		return nil, fmt.Errorf("invalid message length, expected %d, got %d", INDEX_PACKET_LENGTH, len(dataChunks))
	}
	if err := validateDataChunks(dataChunks); err != nil {
		return nil, err
	}

	headerBytes := dataChunks[INDEX_HEADER]
	encryptFlag := dataChunks[INDEX_ENCRYPT_FLAG][0]

	if encryptFlag == ENCRYPT_GLOBALLY {
		var err error
		headerBytes, err = e.decodeWithDecryption(headerBytes, aadDomainHeader, nil, compression.MaxDecodedSize)
		if err != nil {
			return nil, err
		}
	}

	var header Header
	if err := header.Unmarshal(headerBytes); err != nil {
		return nil, err
	}

	return &header, nil
}

func (e *Codec) DecodeMessage(data []byte) (*Message, error) {
	if len(data) == 0 {
		return nil, fmt.Errorf("data is empty")
	}
	if len(data) > maxSignalingPacketSize {
		return nil, fmt.Errorf("signaling packet exceeds %d bytes", maxSignalingPacketSize)
	}

	dec := gob.NewDecoder(bytes.NewBuffer(data))

	var dataChunks [][]byte
	if err := dec.Decode(&dataChunks); err != nil {
		return nil, err
	}
	if len(dataChunks) != INDEX_PACKET_LENGTH {
		return nil, fmt.Errorf("invalid message length, expected %d, got %d", INDEX_PACKET_LENGTH, len(dataChunks))
	}
	if err := validateDataChunks(dataChunks); err != nil {
		return nil, err
	}

	headerBytes := dataChunks[INDEX_HEADER]
	isGloballyEncrypt := dataChunks[INDEX_ENCRYPT_FLAG][0] == ENCRYPT_GLOBALLY

	if isGloballyEncrypt {
		var err error
		headerBytes, err = e.decodeWithDecryption(headerBytes, aadDomainHeader, nil, compression.MaxDecodedSize)
		if err != nil {
			return nil, err
		}
	}

	var header Header
	if err := header.Unmarshal(headerBytes); err != nil {
		return nil, err
	}

	payloadBytes, err := e.decodeWithDecryption(dataChunks[INDEX_PAYLOAD], aadDomainPayload, headerBytes, compression.MaxDecodedSize)
	if err != nil {
		return nil, err
	}

	var payload Payload
	if err := payload.Unmarshal(payloadBytes); err != nil {
		return nil, err
	}

	return &Message{
		Header:  &header,
		Payload: &payload,
		Encrypt: isGloballyEncrypt,
	}, nil
}

func validateDataChunks(dataChunks [][]byte) error {
	if len(dataChunks[INDEX_HEADER]) == 0 || len(dataChunks[INDEX_PAYLOAD]) == 0 {
		return fmt.Errorf("message contains an empty header or payload")
	}
	flag := dataChunks[INDEX_ENCRYPT_FLAG]
	if len(flag) != 1 || (flag[0] != ENCRYPT_PARTIALY && flag[0] != ENCRYPT_GLOBALLY) {
		return fmt.Errorf("message contains an invalid encryption flag")
	}
	return nil
}

func (e *Codec) buildAdditionalData(aad []byte, compression bool, domain byte, context []byte) []byte {
	buf := make([]byte, 0, 24+len(aad)+len(context))
	buf = append(buf, 0x02)                                      // protocol version
	buf = append(buf, byte(lo.Ternary(compression, 0x01, 0x00))) // compression flag
	buf = append(buf, domain)                                    // cryptographic domain
	buf = append(buf, make([]byte, 15)...)                       // reserved for future use
	buf = append(buf, uint8(len(aad)))                           // length of user-defined AAD
	buf = append(buf, aad...)                                    // content of user-defined AAD
	var contextLength [4]byte
	binary.BigEndian.PutUint32(contextLength[:], uint32(len(context)))
	buf = append(buf, contextLength[:]...)
	buf = append(buf, context...)
	return buf
}
