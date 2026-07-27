package main

import (
	"context"
	"errors"
	"github.com/anyshake/telekit/example/p2pssh/streammux"
	"io"
	"net"
	"strings"
)

func isExpectedNetworkError(err error) bool {
	if err == nil {
		return true
	}

	if errors.Is(err, io.EOF) ||
		errors.Is(err, net.ErrClosed) ||
		errors.Is(err, context.Canceled) ||
		errors.Is(err, streammux.ErrClosed) {
		return true
	}

	var networkError *net.OpError

	if errors.As(err, &networkError) &&
		errors.Is(networkError.Err, net.ErrClosed) {
		return true
	}

	message := strings.ToLower(err.Error())

	return strings.Contains(
		message,
		"use of closed network connection",
	) ||
		strings.Contains(
			message,
			"connection reset by peer",
		) ||
		strings.Contains(
			message,
			"broken pipe",
		) ||
		strings.Contains(
			message,
			"operation canceled",
		) ||
		strings.Contains(
			message,
			"operation cancelled",
		)
}
