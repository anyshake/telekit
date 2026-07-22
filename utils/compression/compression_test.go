package compression

import (
	"bytes"
	"fmt"
	"sync"
	"testing"
)

func TestRoundTripConcurrent(t *testing.T) {
	const workers = 16
	var wg sync.WaitGroup
	errs := make(chan error, workers)
	for worker := 0; worker < workers; worker++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			for iteration := 0; iteration < 100; iteration++ {
				payload := bytes.Repeat([]byte{byte(worker), byte(iteration)}, 128)
				encoded, err := Compress(payload)
				if err != nil {
					errs <- err
					return
				}
				decoded, err := Decompress(encoded)
				if err != nil {
					errs <- err
					return
				}
				if !bytes.Equal(decoded, payload) {
					errs <- fmt.Errorf("worker %d iteration %d: round-trip mismatch", worker, iteration)
					return
				}
			}
		}(worker)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Error(err)
	}
}

func TestDecoderReusableAfterInvalidInput(t *testing.T) {
	if _, err := Decompress([]byte("not a zstd frame")); err == nil {
		t.Fatal("invalid compressed data was accepted")
	}
	payload := []byte("valid data after a decode error")
	encoded, err := Compress(payload)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := Decompress(encoded)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(decoded, payload) {
		t.Fatalf("decoded %q, want %q", decoded, payload)
	}
}

func TestDecompressLimitRejectsExpansion(t *testing.T) {
	encoded, err := Compress(bytes.Repeat([]byte("a"), 2048))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := DecompressLimit(encoded, 1024); err == nil {
		t.Fatal("compressed payload expanded beyond configured limit")
	}
	decoded, err := DecompressLimit(encoded, 2048)
	if err != nil || len(decoded) != 2048 {
		t.Fatalf("decode at exact limit = %d bytes, %v", len(decoded), err)
	}
}
