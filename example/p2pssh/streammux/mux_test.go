package streammux

import (
	"bytes"
	"io"
	"net"
	"sync"
	"testing"
	"time"
)

func TestSessionMultiplexesStreams(t *testing.T) {
	left, right := net.Pipe()
	defer left.Close()
	defer right.Close()

	client := NewClient(left)
	defer client.Close()
	server := NewServer(right)
	defer server.Close()

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 2; i++ {
			stream, err := server.Accept()
			if err != nil {
				t.Errorf("Accept: %v", err)
				return
			}
			go func(stream net.Conn) {
				defer stream.Close()
				if _, err := io.Copy(stream, stream); err != nil {
					t.Errorf("echo copy: %v", err)
				}
			}(stream)
		}
	}()

	first, err := client.Dial(nil)
	if err != nil {
		t.Fatal(err)
	}
	defer first.Close()

	second, err := client.Dial(nil)
	if err != nil {
		t.Fatal(err)
	}
	defer second.Close()

	assertEcho(t, first, bytes.Repeat([]byte("a"), 96<<10))
	assertEcho(t, second, []byte("second stream"))

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("server accept loop did not finish")
	}
}

func assertEcho(t *testing.T, conn net.Conn, payload []byte) {
	t.Helper()

	if _, err := conn.Write(payload); err != nil {
		t.Fatal(err)
	}

	got := make([]byte, len(payload))
	if _, err := io.ReadFull(conn, got); err != nil {
		t.Fatal(err)
	}

	if !bytes.Equal(got, payload) {
		t.Fatalf("echo mismatch")
	}
}
