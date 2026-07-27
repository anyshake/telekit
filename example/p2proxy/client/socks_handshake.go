package main

import (
	"encoding/binary"
	"errors"
	"io"
	"net"
	"strconv"
)

func socksHandshake(conn net.Conn) error {
	var header [2]byte
	if _, err := io.ReadFull(conn, header[:]); err != nil || header[0] != 5 {
		return errors.New("invalid SOCKS5 greeting")
	}
	methods := make([]byte, header[1])
	if _, err := io.ReadFull(conn, methods); err != nil {
		return err
	}
	for _, method := range methods {
		if method == 0 {
			return writeFull(conn, []byte{5, 0})
		}
	}
	err := writeFull(conn, []byte{5, 0xff})
	if err == nil {
		err = errors.New("SOCKS5 authentication is unsupported")
	}
	return err
}

func readSOCKSRequest(conn net.Conn) (string, error) {
	var header [4]byte
	if _, err := io.ReadFull(conn, header[:]); err != nil {
		return "", err
	}
	if header[0] != 5 || header[1] != 1 || header[2] != 0 {
		return "", errors.New("only SOCKS5 CONNECT is supported")
	}
	var host string
	switch header[3] {
	case 1:
		address := make([]byte, 4)
		if _, err := io.ReadFull(conn, address); err != nil {
			return "", err
		}
		host = net.IP(address).String()
	case 3:
		var length [1]byte
		if _, err := io.ReadFull(conn, length[:]); err != nil || length[0] == 0 {
			return "", errors.New("invalid SOCKS5 domain")
		}
		address := make([]byte, length[0])
		if _, err := io.ReadFull(conn, address); err != nil {
			return "", err
		}
		host = string(address)
	case 4:
		address := make([]byte, 16)
		if _, err := io.ReadFull(conn, address); err != nil {
			return "", err
		}
		host = net.IP(address).String()
	default:
		return "", errors.New("unsupported SOCKS5 address type")
	}
	var port [2]byte
	if _, err := io.ReadFull(conn, port[:]); err != nil {
		return "", err
	}
	return net.JoinHostPort(host, strconv.Itoa(int(binary.BigEndian.Uint16(port[:])))), nil
}

func writeSOCKSReply(conn net.Conn, code byte) error {
	return writeFull(conn, []byte{5, code, 0, 1, 0, 0, 0, 0, 0, 0})
}

func writeFull(conn net.Conn, data []byte) error {
	for len(data) > 0 {
		n, err := conn.Write(data)
		if n > 0 {
			data = data[n:]
		}
		if err != nil {
			return err
		}
		if n == 0 {
			return io.ErrShortWrite
		}
	}
	return nil
}
