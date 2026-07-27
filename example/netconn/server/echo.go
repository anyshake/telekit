package main

import (
	"errors"
	"io"
	"log"
	"net"
)

func acceptLoop(listener net.Listener) {
	for {
		conn, err := listener.Accept()
		if err != nil {
			if !errors.Is(err, net.ErrClosed) {
				log.Printf("connection: accept failed: %v", err)
			}
			return
		}
		go echo(conn)
	}
}

func echo(conn net.Conn) {
	defer conn.Close()
	log.Printf("connection: opened local=%s remote=%s", conn.LocalAddr(), conn.RemoteAddr())
	if _, err := io.Copy(conn, conn); err != nil && !errors.Is(err, net.ErrClosed) {
		log.Printf("data: echo failed remote=%s: %v", conn.RemoteAddr(), err)
	}
	log.Printf("connection: closed remote=%s", conn.RemoteAddr())
}
