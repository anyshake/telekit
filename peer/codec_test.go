package peer

import (
	"bytes"
	"encoding/gob"
	"testing"

	"github.com/anyshake/telekit/utils/encryption"
)

func TestDecodeRejectsMalformedEncryptionFlag(t *testing.T) {
	var wire bytes.Buffer
	if err := gob.NewEncoder(&wire).Encode([][]byte{{1}, {1}, {}}); err != nil {
		t.Fatal(err)
	}
	codec := &Codec{}
	if _, err := codec.DecodeMessageHeader(wire.Bytes()); err == nil {
		t.Fatal("malformed encryption flag was accepted")
	}
}

func TestDecodeRejectsTamperedPlaintextHeader(t *testing.T) {
	codec := testCodec(t)
	encoded, err := codec.EncodeMessage(&Message{
		Header:  &Header{Type: MessageTypeICE, SourceId: "client", TargetId: "server", Sequence: 1},
		Payload: &Payload{SessionSalt: []byte("authenticated payload")},
	})
	if err != nil {
		t.Fatal(err)
	}

	chunks := decodeWireChunks(t, encoded)
	chunks[INDEX_HEADER] = (&Header{
		Type: MessageTypeOffer, SourceId: "client", TargetId: "server", Sequence: 1,
	}).Marshal()
	if _, err := codec.DecodeMessage(encodeWireChunks(t, chunks)); err == nil {
		t.Fatal("payload was accepted after its plaintext routing header was modified")
	}
}

func TestDecodeRejectsEncryptedHeaderPayloadSplice(t *testing.T) {
	codec := testCodec(t)
	first, err := codec.EncodeMessage(&Message{
		Header:  &Header{Type: MessageTypeAnswer, SourceId: "server", TargetId: "client", Sequence: 1},
		Payload: &Payload{SessionSalt: []byte("first")},
		Encrypt: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	second, err := codec.EncodeMessage(&Message{
		Header:  &Header{Type: MessageTypeICE, SourceId: "server", TargetId: "client", Sequence: 2},
		Payload: &Payload{SessionSalt: []byte("second")},
		Encrypt: true,
	})
	if err != nil {
		t.Fatal(err)
	}

	firstChunks := decodeWireChunks(t, first)
	secondChunks := decodeWireChunks(t, second)
	firstChunks[INDEX_PAYLOAD] = secondChunks[INDEX_PAYLOAD]
	if _, err := codec.DecodeMessage(encodeWireChunks(t, firstChunks)); err == nil {
		t.Fatal("payload from another encrypted message was accepted with a different header")
	}
}

func testCodec(t *testing.T) *Codec {
	t.Helper()
	codec, err := NewCodec(
		encryption.XCHACHA20_POLY1305,
		bytes.Repeat([]byte{0x42}, 32),
		[]byte("test"),
		false,
	)
	if err != nil {
		t.Fatal(err)
	}
	return codec
}

func decodeWireChunks(t *testing.T, wire []byte) [][]byte {
	t.Helper()
	var chunks [][]byte
	if err := gob.NewDecoder(bytes.NewReader(wire)).Decode(&chunks); err != nil {
		t.Fatal(err)
	}
	return chunks
}

func encodeWireChunks(t *testing.T, chunks [][]byte) []byte {
	t.Helper()
	var wire bytes.Buffer
	if err := gob.NewEncoder(&wire).Encode(chunks); err != nil {
		t.Fatal(err)
	}
	return wire.Bytes()
}

func BenchmarkCodecSmallFrame(b *testing.B) {
	for _, compressed := range []bool{false, true} {
		name := "uncompressed"
		if compressed {
			name = "compressed"
		}
		b.Run(name, func(b *testing.B) {
			codec, err := NewCodec(
				encryption.XCHACHA20_POLY1305,
				bytes.Repeat([]byte{0x42}, 32),
				[]byte("benchmark"),
				compressed,
			)
			if err != nil {
				b.Fatal(err)
			}
			payload := bytes.Repeat([]byte("x"), 256)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				encoded, err := codec.EncodeWithEncryption(payload)
				if err != nil {
					b.Fatal(err)
				}
				if _, err := codec.DecodeWithDecryption(encoded); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
