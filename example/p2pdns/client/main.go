package main

import (
	"context"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"
)

func init() {
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)
	log.SetPrefix("[telekit p2pdns/client] ")
}

func main() {
	args := parseCliArguments()
	localAddr, err := net.ResolveUDPAddr("udp", args.localDNS)
	if err != nil {
		log.Fatalln(err)
	}
	local, err := net.ListenUDP("udp", localAddr)
	if err != nil {
		log.Fatalln(err)
	}
	defer local.Close()
	adapter, conn, remote, err := connectTelekit(args)
	if err != nil {
		log.Fatalln(err)
	}
	defer adapter.Disconnect()
	defer conn.Close()

	proxy := newDNSClient(local, remote, 5*time.Second)
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	log.Printf("ready: local=%s remote=%s transport=raw_udp", local.LocalAddr(), conn.RemoteAddr())
	if err := proxy.Run(ctx); err != nil {
		log.Fatalln(err)
	}
}
