package main

import (
	"context"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"
)

func init() {
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)
	log.SetPrefix("[telekit proxy/client] ")
}

func main() {
	args := parseCliArguments()
	adapter, pool, err := connectTelekit(args)
	if err != nil {
		log.Fatalln(err)
	}
	defer adapter.Disconnect()
	defer pool.Close()
	listener, err := net.Listen("tcp", args.socksAddr)
	if err != nil {
		log.Fatalln(err)
	}
	defer listener.Close()
	log.Printf("ready: SOCKS5 listening on %s via transport=%s client-id=%s pool_size=%d",
		args.socksAddr, args.transportName, args.clientID, args.poolSize)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	go acceptSOCKS(ctx, listener, pool)
	select {
	case <-ctx.Done():
	case <-pool.Done():
		log.Printf("telekit session pool closed")
	}
}
