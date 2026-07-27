package main

import (
	"errors"
	"fmt"
	"log"
	"net"
	"strings"
	"time"

	"github.com/anyshake/telekit/example/p2pssh/streammux"
	"golang.org/x/crypto/ssh"
)

type directTCPIPRequest struct {
	Host       string
	Port       uint32
	OriginHost string
	OriginPort uint32
}

func acceptLoop(
	listener net.Listener,
	sshConfig *ssh.ServerConfig,
	shell string,
	allowTCPForwarding bool,
	forwardDialTimeout time.Duration,
) {
	for {
		conn, err := listener.Accept()
		if err != nil {
			if !errors.Is(err, net.ErrClosed) {
				log.Printf(
					"connection: accept failed: %v",
					err,
				)
			}

			return
		}

		go handleMultiplexedSession(
			conn,
			sshConfig,
			shell,
			allowTCPForwarding,
			forwardDialTimeout,
		)
	}
}

func handleMultiplexedSession(
	conn net.Conn,
	sshConfig *ssh.ServerConfig,
	shell string,
	allowTCPForwarding bool,
	forwardDialTimeout time.Duration,
) {
	mux := streammux.NewServer(conn)
	defer mux.Close()

	log.Printf(
		"multiplex session opened: addr=%s",
		conn.RemoteAddr(),
	)

	for {
		stream, err := mux.Accept()
		if err != nil {
			if !isExpectedNetworkError(err) &&
				!errors.Is(err, streammux.ErrClosed) {
				log.Printf(
					"multiplex session accept failed: addr=%s err=%v",
					conn.RemoteAddr(),
					err,
				)
			}

			log.Printf(
				"multiplex session closed: addr=%s",
				conn.RemoteAddr(),
			)

			return
		}

		go handleSSHConnection(
			stream,
			sshConfig,
			shell,
			allowTCPForwarding,
			forwardDialTimeout,
		)
	}
}

func handleSSHConnection(
	networkConn net.Conn,
	sshConfig *ssh.ServerConfig,
	shell string,
	allowTCPForwarding bool,
	forwardDialTimeout time.Duration,
) {
	defer networkConn.Close()

	sshConn, channels, requests, err := ssh.NewServerConn(
		networkConn,
		sshConfig,
	)
	if err != nil {
		log.Printf(
			"SSH handshake failed: addr=%s err=%v",
			networkConn.RemoteAddr(),
			err,
		)
		return
	}

	defer func() {
		_ = sshConn.Close()
	}()

	go ssh.DiscardRequests(requests)

	for newChannel := range channels {
		switch newChannel.ChannelType() {
		case "session":
			channel, channelRequests, err := newChannel.Accept()
			if err != nil {
				log.Printf(
					"session channel accept failed: user=%s addr=%s err=%v",
					sshConn.User(),
					sshConn.RemoteAddr(),
					err,
				)
				continue
			}

			go handleSession(
				channel,
				channelRequests,
				shell,
			)

		case "direct-tcpip":
			if !allowTCPForwarding {
				log.Printf(
					"direct-tcpip rejected: forwarding disabled user=%s addr=%s",
					sshConn.User(),
					sshConn.RemoteAddr(),
				)

				_ = newChannel.Reject(
					ssh.Prohibited,
					"TCP forwarding is disabled",
				)
				continue
			}

			request, err := parseDirectTCPIPRequest(
				newChannel.ExtraData(),
			)
			if err != nil {
				log.Printf(
					"direct-tcpip rejected: invalid request user=%s addr=%s err=%v",
					sshConn.User(),
					sshConn.RemoteAddr(),
					err,
				)

				_ = newChannel.Reject(
					ssh.Prohibited,
					"invalid direct-tcpip request",
				)
				continue
			}

			go handleDirectTCPIP(
				newChannel,
				request,
				sshConn.User(),
				sshConn.RemoteAddr(),
				forwardDialTimeout,
			)

		default:
			_ = newChannel.Reject(
				ssh.UnknownChannelType,
				fmt.Sprintf(
					"unsupported channel type %q",
					newChannel.ChannelType(),
				),
			)
		}
	}
}

func parseDirectTCPIPRequest(
	payload []byte,
) (directTCPIPRequest, error) {
	var request directTCPIPRequest

	if err := ssh.Unmarshal(payload, &request); err != nil {
		return directTCPIPRequest{}, fmt.Errorf(
			"decode direct-tcpip request: %w",
			err,
		)
	}

	request.Host = strings.TrimSpace(request.Host)
	request.OriginHost = strings.TrimSpace(request.OriginHost)

	if request.Host == "" {
		return directTCPIPRequest{}, errors.New(
			"target host is empty",
		)
	}

	if request.Port == 0 || request.Port > 65535 {
		return directTCPIPRequest{}, fmt.Errorf(
			"invalid target port %d",
			request.Port,
		)
	}

	if request.OriginPort > 65535 {
		return directTCPIPRequest{}, fmt.Errorf(
			"invalid origin port %d",
			request.OriginPort,
		)
	}

	return request, nil
}
