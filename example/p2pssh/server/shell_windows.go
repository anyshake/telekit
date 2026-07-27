//go:build windows

package main

import "golang.org/x/crypto/ssh"

// Windows builds intentionally expose SFTP and direct-tcpip only. PTY and
// shell execution require Unix process and terminal semantics.
func handleSession(channel ssh.Channel, requests <-chan *ssh.Request, _ string) {
	defer channel.Close()
	for request := range requests {
		if request.Type == "subsystem" {
			if err := handleSFTP(channel, request); err != nil {
				return
			}
			return
		}
		_ = request.Reply(false, nil)
	}
}
