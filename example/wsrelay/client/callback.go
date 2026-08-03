package main

import (
	"log"

	"github.com/anyshake/telekit/peer/client"
)

func handleClientHello(*client.Client) { log.Println("handshake: ClientHello sent") }

func handleServerHello(*client.Client) { log.Println("handshake: ServerHello received") }

func handleConnectionFailed(_ *client.Client, err error) {
	log.Printf("connection: failed: %v", err)
}

func handleDisconnected(disconnected chan<- struct{}) func(*client.Client) {
	return func(*client.Client) {
		log.Println("connection: disconnected")
		// The example has one client connection. Notify the main loop so it
		// does not remain blocked waiting for stdin after the connection ends.
		select {
		case disconnected <- struct{}{}:
		default:
		}
	}
}
