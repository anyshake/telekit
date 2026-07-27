//go:build !windows

package main

import (
	"io"
	"log"
	"os"
	"os/exec"
	"sync"
	"syscall"

	"github.com/creack/pty"
	"golang.org/x/crypto/ssh"
)

func handleSession(
	channel ssh.Channel,
	requests <-chan *ssh.Request,
	shell string,
) {
	defer channel.Close()

	var (
		command *exec.Cmd
		ptyFile *os.File
		once    sync.Once
	)

	initialWidth := 80
	initialHeight := 24
	terminal := "xterm-256color"

	closeAll := func() {
		once.Do(func() {
			if ptyFile != nil {
				_ = ptyFile.Close()
			}

			_ = channel.Close()

			if command != nil && command.Process != nil {
				_ = command.Process.Kill()
				_, _ = command.Process.Wait()
			}
		})
	}

	for request := range requests {
		switch request.Type {
		case "pty-req":
			term, width, height, err := parsePtyReq(
				request.Payload,
			)
			if err != nil {
				_ = request.Reply(false, nil)
				continue
			}

			if width > 0 {
				initialWidth = width
			}

			if height > 0 {
				initialHeight = height
			}

			if term != "" {
				terminal = term
			}

			_ = request.Reply(true, nil)

		case "window-change":
			width, height, err := parseWinch(
				request.Payload,
			)
			if err != nil {
				continue
			}

			if ptyFile != nil {
				_ = pty.Setsize(
					ptyFile,
					&pty.Winsize{
						Cols: uint16(width),
						Rows: uint16(height),
					},
				)
			}

		case "shell":
			if command != nil {
				_ = request.Reply(false, nil)
				continue
			}

			_ = request.Reply(true, nil)

			command = exec.Command(shell)
			command.Env = append(
				os.Environ(),
				"TERM="+terminal,
			)
			command.SysProcAttr = &syscall.SysProcAttr{
				Setsid: true,
			}

			var err error

			ptyFile, err = pty.Start(command)
			if err != nil {
				log.Printf(
					"start shell %q failed: %v",
					shell,
					err,
				)

				closeAll()
				return
			}

			_ = pty.Setsize(
				ptyFile,
				&pty.Winsize{
					Cols: uint16(initialWidth),
					Rows: uint16(initialHeight),
				},
			)

			log.Println("shell started")

			done := make(chan struct{})

			go func() {
				_, err := io.Copy(ptyFile, channel)
				if err != nil &&
					!isExpectedNetworkError(err) {
					log.Printf(
						"copy SSH -> PTY failed: %v",
						err,
					)
				}

				done <- struct{}{}
			}()

			go func() {
				_, err := io.Copy(channel, ptyFile)
				if err != nil &&
					!isExpectedNetworkError(err) {
					log.Printf(
						"copy PTY -> SSH failed: %v",
						err,
					)
				}

				done <- struct{}{}
			}()

			go handleRunningSessionRequests(
				requests,
				command,
			)

			<-done
			closeAll()

			log.Println("shell exited")
			return

		case "subsystem":
			if err := handleSFTP(channel, request); err != nil {
				log.Printf("SFTP session failed: %v", err)
			}
			return

		default:
			_ = request.Reply(false, nil)
		}
	}
}

func handleRunningSessionRequests(
	requests <-chan *ssh.Request,
	command *exec.Cmd,
) {
	for request := range requests {
		switch request.Type {
		case "signal":
			signal, err := parseSignal(request.Payload)
			if err != nil {
				_ = request.Reply(false, nil)
				continue
			}

			if command.Process != nil {
				_ = command.Process.Signal(signal)
			}

			_ = request.Reply(true, nil)

		default:
			_ = request.Reply(false, nil)
		}
	}
}
