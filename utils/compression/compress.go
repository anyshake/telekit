package compression

import (
	"sync"

	"github.com/klauspost/compress/zstd"
)

var encoderPool sync.Pool

func Compress(data []byte) ([]byte, error) {
	encoder, _ := encoderPool.Get().(*zstd.Encoder)
	if encoder == nil {
		var err error
		encoder, err = zstd.NewWriter(
			nil,
			zstd.WithEncoderLevel(zstd.SpeedFastest),
			zstd.WithEncoderConcurrency(1),
			zstd.WithEncoderCRC(false),
		)
		if err != nil {
			return nil, err
		}
	}
	defer encoderPool.Put(encoder)

	return encoder.EncodeAll(data, nil), nil
}
