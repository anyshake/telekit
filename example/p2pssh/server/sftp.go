package main

import (
	"errors"
	"io"
	"log"

	"github.com/pkg/sftp"
	"golang.org/x/crypto/ssh"
)

func handleSFTP(channel ssh.Channel, request *ssh.Request) error {
	subsystem, err := parseSubsystem(request.Payload)
	if err != nil || subsystem != "sftp" {
		_ = request.Reply(false, nil)
		if err == nil {
			err = errors.New("unsupported SSH subsystem")
		}
		return err
	}
	if err := request.Reply(true, nil); err != nil {
		return err
	}

	log.Println("SFTP session started")
	server, err := sftp.NewServer(channel)
	if err != nil {
		return err
	}
	if err := server.Serve(); err != nil && !errors.Is(err, io.EOF) && !isExpectedNetworkError(err) {
		return err
	}
	_ = server.Close()
	log.Println("SFTP session closed")
	return nil
}
