package main

import (
	"context"
	"errors"
	"io"
	"log"
	"net"
	"strings"
)

func isExpectedNetworkError(err error) bool {
	if err == nil {
		return true
	}

	if errors.Is(err, io.EOF) ||
		errors.Is(err, net.ErrClosed) ||
		errors.Is(err, context.Canceled) {
		return true
	}

	var netErr *net.OpError
	if errors.As(err, &netErr) &&
		errors.Is(netErr.Err, net.ErrClosed) {
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

func logCopyResult(
	localAddr string,
	clientID string,
	direction string,
	bytes int64,
	err error,
	ctxErr error,
) {
	if err != nil &&
		ctxErr == nil &&
		!isExpectedNetworkError(err) {
		log.Printf(
			"proxy[%s]: copy %s failed client_id=%q after %d bytes: %v",
			localAddr,
			direction,
			clientID,
			bytes,
			err,
		)
		return
	}

	log.Printf(
		"proxy[%s]: copy %s ended client_id=%q after %d bytes",
		localAddr,
		direction,
		clientID,
		bytes,
	)
}
