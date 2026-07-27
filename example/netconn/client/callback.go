package main

import (
	"log"

	"github.com/anyshake/telekit/peer/client"
)

func handleClientHello(*client.Client) {
	log.Println("client hello sent")
}

func handleServerHello(*client.Client) {
	log.Println("server hello received")
}

func handleConnectionFailed(c *client.Client, err error) {
	log.Printf("connection error: %v", err)
}

func handleDisconnect(*client.Client) {
	log.Println("connection lost")
}
