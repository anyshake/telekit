package main

import (
	"encoding/binary"
	"errors"
)

func parseSubsystem(payload []byte) (string, error) {
	if len(payload) < 4 {
		return "", errors.New(
			"subsystem payload too short",
		)
	}

	length := binary.BigEndian.Uint32(payload[:4])

	if uint64(length)+4 > uint64(len(payload)) {
		return "", errors.New(
			"invalid subsystem string length",
		)
	}

	return string(payload[4 : 4+length]), nil
}
