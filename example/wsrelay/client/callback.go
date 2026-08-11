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

func handleDisconnected(*client.Client) {
	log.Println("connection: disconnected; renegotiating ICE")
}
