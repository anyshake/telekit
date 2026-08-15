package protocol

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"strconv"
)

// ReadUDPFrame reads the UDP-in-TCP framing emitted by Hev tun2socks.
func ReadUDPFrame(reader io.Reader) (string, []byte, error) {
	var header [5]byte
	if _, err := io.ReadFull(reader, header[:]); err != nil {
		return "", nil, err
	}
	dataLen := int(binary.BigEndian.Uint16(header[:2]))
	addressLen := int(header[2]) - 3
	if addressLen < 2 || addressLen > 263 {
		return "", nil, errors.New("invalid Hev UDP address length")
	}
	body := make([]byte, addressLen-2+dataLen)
	if _, err := io.ReadFull(reader, body); err != nil {
		return "", nil, err
	}
	address := make([]byte, addressLen)
	copy(address, header[3:5])
	copy(address[2:], body[:addressLen-2])
	target, err := decodeUDPAddress(address)
	if err != nil {
		return "", nil, err
	}
	return target, body[addressLen-2:], nil
}

func decodeUDPAddress(address []byte) (string, error) {
	if len(address) < 2 {
		return "", errors.New("invalid Hev UDP address")
	}
	var host string
	var portOffset int
	switch address[0] {
	case 1:
		if len(address) != 7 {
			return "", errors.New("invalid Hev IPv4 address")
		}
		host = net.IP(address[1:5]).String()
		portOffset = 5
	case 4:
		if len(address) != 19 {
			return "", errors.New("invalid Hev IPv6 address")
		}
		host = net.IP(address[1:17]).String()
		portOffset = 17
	case 3:
		nameLen := int(address[1])
		if len(address) != 4+nameLen {
			return "", errors.New("invalid Hev domain address")
		}
		host = string(address[2 : 2+nameLen])
		portOffset = 2 + nameLen
	default:
		return "", errors.New("unsupported Hev UDP address type")
	}
	port := int(binary.BigEndian.Uint16(address[portOffset:]))
	return net.JoinHostPort(host, strconv.Itoa(port)), nil
}

// WriteUDPFrame emits a UDP-in-TCP frame accepted by Hev tun2socks.
func WriteUDPFrame(writer io.Writer, addr *net.UDPAddr, payload []byte) error {
	var address []byte
	if ip := addr.IP.To4(); ip != nil {
		address = make([]byte, 7)
		address[0] = 1
		copy(address[1:5], ip)
	} else if ip := addr.IP.To16(); ip != nil {
		address = make([]byte, 19)
		address[0] = 4
		copy(address[1:17], ip)
	} else {
		return errors.New("UDP response has no IP address")
	}
	binary.BigEndian.PutUint16(address[len(address)-2:], uint16(addr.Port))
	if len(payload) > 65535 {
		return fmt.Errorf("UDP payload is too large: %d", len(payload))
	}
	frame := make([]byte, 3+len(address)+len(payload))
	binary.BigEndian.PutUint16(frame[:2], uint16(len(payload)))
	frame[2] = uint8(3 + len(address))
	copy(frame[3:], address)
	copy(frame[3+len(address):], payload)
	return writeFrameFull(writer, frame)
}

func writeFrameFull(writer io.Writer, data []byte) error {
	for len(data) > 0 {
		n, err := writer.Write(data)
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
