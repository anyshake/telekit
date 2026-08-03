package main

import "flag"

const demoServerPublicKey = "2dc55c63afa1d2ca5d958acf19dafbbf3f77b7752a5204e8ceb881d1cc1b7643"

type arguments struct {
	listenAddress   string
	relayAddress    string
	token           string
	serverPublicKey string
	clientIDs       string
	sessionPrefix   string
	pathPrefix      string
}

func parseCLIArguments() *arguments {
	args := &arguments{}
	flag.StringVar(&args.listenAddress, "listen", "0.0.0.0:8080", "HTTP/WebSocket listen address")
	flag.StringVar(&args.relayAddress, "relay-address", "127.0.0.1", "address published in ICE relay candidates")
	flag.StringVar(&args.token, "token", "change@me", "relay authorization token")
	flag.StringVar(&args.serverPublicKey, "server-public-key", demoServerPublicKey, "pinned server public key (hex)")
	flag.StringVar(&args.clientIDs, "client-ids", "client-a", "comma-separated authorized client IDs")
	flag.StringVar(&args.sessionPrefix, "session-prefix", "example-wsrelay", "relay session prefix")
	flag.StringVar(&args.pathPrefix, "path-prefix", "/relay", "relay URL path prefix; server ID is appended")
	flag.Parse()
	return args
}
