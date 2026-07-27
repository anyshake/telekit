package common

import (
	"encoding/binary"
	"errors"
	"io"
	"net"
)

const MaxDNSPacketSize = 64 << 10

func ReadDNSPacket(conn net.Conn) ([]byte, error) {
	var header [4]byte
	if _, err := io.ReadFull(conn, header[:]); err != nil {
		return nil, err
	}
	length := binary.BigEndian.Uint32(header[:])
	if length == 0 || length > MaxDNSPacketSize {
		return nil, errors.New("invalid DNS packet length")
	}
	packet := make([]byte, length)
	if _, err := io.ReadFull(conn, packet); err != nil {
		return nil, err
	}
	return packet, nil
}

func WriteDNSPacket(conn net.Conn, packet []byte) error {
	if len(packet) == 0 || len(packet) > MaxDNSPacketSize {
		return errors.New("invalid DNS packet length")
	}
	frame := make([]byte, 4+len(packet))
	binary.BigEndian.PutUint32(frame[:4], uint32(len(packet)))
	copy(frame[4:], packet)
	for len(frame) > 0 {
		n, err := conn.Write(frame)
		if err != nil {
			return err
		}
		if n == 0 {
			return io.ErrShortWrite
		}
		frame = frame[n:]
	}
	return nil
}
