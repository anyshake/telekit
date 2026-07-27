package main

import (
	"fmt"
	"io"
	"log"
	"net"
	"strconv"
	"sync"
	"time"

	"golang.org/x/crypto/ssh"
)

type copyResult struct {
	direction string
	bytes     int64
	err       error
}

func handleDirectTCPIP(
	newChannel ssh.NewChannel,
	request directTCPIPRequest,
	username string,
	sshRemoteAddr net.Addr,
	dialTimeout time.Duration,
) {
	targetAddress := net.JoinHostPort(
		request.Host,
		strconv.FormatUint(uint64(request.Port), 10),
	)

	originAddress := net.JoinHostPort(
		request.OriginHost,
		strconv.FormatUint(uint64(request.OriginPort), 10),
	)

	log.Printf(
		"direct-tcpip: dialing target=%s origin=%s user=%s ssh_addr=%s",
		targetAddress,
		originAddress,
		username,
		sshRemoteAddr,
	)

	dialer := net.Dialer{
		Timeout:   dialTimeout,
		KeepAlive: 30 * time.Second,
	}

	targetConn, err := dialer.Dial(
		"tcp",
		targetAddress,
	)
	if err != nil {
		log.Printf(
			"direct-tcpip: dial failed target=%s origin=%s user=%s err=%v",
			targetAddress,
			originAddress,
			username,
			err,
		)

		_ = newChannel.Reject(
			ssh.ConnectionFailed,
			fmt.Sprintf(
				"connect to %s failed",
				targetAddress,
			),
		)
		return
	}

	channel, requests, err := newChannel.Accept()
	if err != nil {
		_ = targetConn.Close()

		log.Printf(
			"direct-tcpip: accept channel failed target=%s origin=%s user=%s err=%v",
			targetAddress,
			originAddress,
			username,
			err,
		)

		return
	}

	go ssh.DiscardRequests(requests)

	log.Printf(
		"direct-tcpip: connected target=%s origin=%s user=%s",
		targetAddress,
		originAddress,
		username,
	)

	proxySSHChannel(
		channel,
		targetConn,
		targetAddress,
		originAddress,
		username,
	)

	log.Printf(
		"direct-tcpip: closed target=%s origin=%s user=%s",
		targetAddress,
		originAddress,
		username,
	)
}

func proxySSHChannel(
	channel ssh.Channel,
	target net.Conn,
	targetAddress string,
	originAddress string,
	username string,
) {
	results := make(chan copyResult, 2)

	var closeOnce sync.Once

	closeBoth := func() {
		closeOnce.Do(func() {
			_ = channel.Close()
			_ = target.Close()
		})
	}

	copyData := func(
		direction string,
		dst io.Writer,
		src io.Reader,
	) {
		bytesCopied, err := io.Copy(dst, src)

		results <- copyResult{
			direction: direction,
			bytes:     bytesCopied,
			err:       err,
		}
	}

	go copyData(
		"ssh -> target",
		target,
		channel,
	)

	go copyData(
		"target -> ssh",
		channel,
		target,
	)

	first := <-results

	logForwardCopyResult(
		targetAddress,
		originAddress,
		username,
		first,
	)

	closeBoth()

	second := <-results

	logForwardCopyResult(
		targetAddress,
		originAddress,
		username,
		second,
	)
}

func logForwardCopyResult(
	targetAddress string,
	originAddress string,
	username string,
	result copyResult,
) {
	if result.err != nil &&
		!isExpectedNetworkError(result.err) {
		log.Printf(
			"direct-tcpip: copy %s failed target=%s origin=%s user=%s after=%d err=%v",
			result.direction,
			targetAddress,
			originAddress,
			username,
			result.bytes,
			result.err,
		)
		return
	}

	log.Printf(
		"direct-tcpip: copy %s ended target=%s origin=%s user=%s after=%d",
		result.direction,
		targetAddress,
		originAddress,
		username,
		result.bytes,
	)
}
