package compression

import (
	"sync"

	"github.com/klauspost/compress/zstd"
)

const MaxDecodedSize = 4 << 20

var decoderPool sync.Pool

func Decompress(data []byte) ([]byte, error) {
	return DecompressLimit(data, MaxDecodedSize)
}

func DecompressLimit(data []byte, limit int) ([]byte, error) {
	if limit <= 0 || limit > MaxDecodedSize {
		limit = MaxDecodedSize
	}
	decoder, _ := decoderPool.Get().(*zstd.Decoder)
	if decoder == nil {
		var err error
		decoder, err = zstd.NewReader(
			nil,
			zstd.WithDecoderConcurrency(1),
			zstd.WithDecoderLowmem(true),
			zstd.WithDecoderMaxMemory(MaxDecodedSize),
			zstd.WithDecodeAllCapLimit(true),
		)
		if err != nil {
			return nil, err
		}
	}
	defer decoderPool.Put(decoder)

	return decoder.DecodeAll(data, make([]byte, 0, limit))
}
