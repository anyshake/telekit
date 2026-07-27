package main

import peerapi "github.com/anyshake/telekit/peer/api"

func apiOptions() []peerapi.Option {
	return []peerapi.Option{peerapi.WithSTUNServer(
		"stun://stun.l.google.com:19302",
		"stun://stun.cloudflare.com:3478",
		"stun://global.stun.twilio.com:3478",
	)}
}
