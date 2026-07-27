//go:build !windows

package main

import (
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"syscall"
)

func parseWinch(payload []byte) (int, int, error) {
	if len(payload) < 8 {
		return 0, 0, errors.New("window-change payload too short")
	}
	width := int(binary.BigEndian.Uint32(payload[:4]))
	height := int(binary.BigEndian.Uint32(payload[4:8]))
	if width <= 0 {
		width = 80
	}
	if height <= 0 {
		height = 24
	}
	return width, height, nil
}

func parsePtyReq(payload []byte) (string, int, int, error) {
	if len(payload) < 4 {
		return "", 0, 0, errors.New("pty-req payload too short")
	}
	terminalLength := binary.BigEndian.Uint32(payload[:4])
	if uint64(terminalLength)+4 > uint64(len(payload)) {
		return "", 0, 0, errors.New("invalid terminal string length")
	}
	offset := 4 + int(terminalLength)
	if len(payload)-offset < 8 {
		return "", 0, 0, errors.New("pty-req dimensions missing")
	}
	terminal := string(payload[4:offset])
	if terminal == "" {
		terminal = "xterm-256color"
	}
	width := int(binary.BigEndian.Uint32(payload[offset : offset+4]))
	height := int(binary.BigEndian.Uint32(payload[offset+4 : offset+8]))
	if width <= 0 {
		width = 80
	}
	if height <= 0 {
		height = 24
	}
	return terminal, width, height, nil
}

func parseSignal(payload []byte) (os.Signal, error) {
	if len(payload) < 4 {
		return nil, errors.New("signal payload too short")
	}
	length := binary.BigEndian.Uint32(payload[:4])
	if uint64(length)+4 > uint64(len(payload)) {
		return nil, errors.New("invalid signal string length")
	}
	switch string(payload[4 : 4+length]) {
	case "ABRT":
		return syscall.SIGABRT, nil
	case "ALRM":
		return syscall.SIGALRM, nil
	case "FPE":
		return syscall.SIGFPE, nil
	case "HUP":
		return syscall.SIGHUP, nil
	case "ILL":
		return syscall.SIGILL, nil
	case "INT":
		return syscall.SIGINT, nil
	case "KILL":
		return syscall.SIGKILL, nil
	case "PIPE":
		return syscall.SIGPIPE, nil
	case "QUIT":
		return syscall.SIGQUIT, nil
	case "SEGV":
		return syscall.SIGSEGV, nil
	case "TERM":
		return syscall.SIGTERM, nil
	case "USR1":
		return syscall.SIGUSR1, nil
	case "USR2":
		return syscall.SIGUSR2, nil
	default:
		return nil, fmt.Errorf("unsupported signal %q", string(payload[4:4+length]))
	}
}
