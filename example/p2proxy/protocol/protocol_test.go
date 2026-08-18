package protocol

import (
	"context"
	"errors"
	"io"
	"net"
	"sync"
	"testing"
	"time"
)

func TestSessionMultiplexesStreams(t *testing.T) {
	left, right := net.Pipe()
	client := NewSession(left)
	server := NewSession(right)
	defer client.Close()
	defer server.Close()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	type openResult struct {
		stream *Stream
		err    error
	}
	opened := make(chan openResult, 1)
	go func() {
		stream, err := client.Open(ctx, "example.com:443")
		opened <- openResult{stream: stream, err: err}
	}()
	request, err := server.Accept(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if request.Address != "example.com:443" {
		t.Fatalf("target = %q", request.Address)
	}
	if err := request.Stream.Accept(); err != nil {
		t.Fatal(err)
	}
	result := <-opened
	if result.err != nil {
		t.Fatal(result.err)
	}
	clientStream := result.stream

	if _, err := clientStream.Write([]byte("request")); err != nil {
		t.Fatal(err)
	}
	data := make([]byte, len("request"))
	if _, err := io.ReadFull(request.Stream, data); err != nil {
		t.Fatal(err)
	}
	if string(data) != "request" {
		t.Fatalf("request data = %q", data)
	}
	if _, err := request.Stream.Write([]byte("response")); err != nil {
		t.Fatal(err)
	}
	if err := request.Stream.CloseWrite(); err != nil {
		t.Fatal(err)
	}
	data = make([]byte, len("response"))
	if _, err := io.ReadFull(clientStream, data); err != nil {
		t.Fatal(err)
	}
	if string(data) != "response" {
		t.Fatalf("response data = %q", data)
	}
	if _, err := clientStream.Read(make([]byte, 1)); err != io.EOF {
		t.Fatalf("remote write close read error = %v", err)
	}
	_ = clientStream.Close()
	if _, err := request.Stream.Read(make([]byte, 1)); err != io.EOF {
		t.Fatalf("remote close read error = %v", err)
	}
}

func TestPoolRetriesUnavailableSession(t *testing.T) {
	failedClient, failedPeer := net.Pipe()
	failedConn := &writeFailConn{Conn: failedClient, writeStarted: make(chan struct{})}
	failed := NewSession(failedConn)
	defer failedPeer.Close()

	slot := NewSessionSlot()
	slot.Replace(failed)
	pool := NewPoolSlots(slot)
	defer pool.Close()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	opened := make(chan struct {
		stream *Stream
		err    error
	}, 1)
	go func() {
		stream, err := pool.Open(ctx, "example.com:443")
		opened <- struct {
			stream *Stream
			err    error
		}{stream: stream, err: err}
	}()

	select {
	case <-failedConn.writeStarted:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	workingClient, workingPeer := net.Pipe()
	working := NewSession(workingClient)
	workingServer := NewSession(workingPeer)
	defer workingServer.Close()
	go func() {
		request, err := workingServer.Accept(ctx)
		if err == nil {
			_ = request.Stream.Accept()
		}
	}()
	slot.Replace(working)

	result := <-opened
	if result.err != nil {
		t.Fatal(result.err)
	}
	if result.stream == nil {
		t.Fatal("pool returned a nil stream")
	}
	_ = result.stream.Close()
}

type writeFailConn struct {
	net.Conn
	writeStarted chan struct{}
	writeOnce    sync.Once
}

func (c *writeFailConn) Write([]byte) (int, error) {
	c.writeOnce.Do(func() { close(c.writeStarted) })
	return 0, errors.New("transport not connected")
}
