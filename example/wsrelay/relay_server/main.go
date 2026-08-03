package main

import (
	"context"
	"crypto/ed25519"
	"encoding/hex"
	"errors"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	pathpkg "path"
	"slices"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/anyshake/telekit/peer"
	relayws "github.com/anyshake/telekit/relays/websocket"
	"github.com/gin-gonic/gin"
)

func main() {
	args := parseCLIArguments()

	_, listenPort, err := net.SplitHostPort(args.listenAddress)
	if err != nil || listenPort == "" {
		log.Fatalf("invalid listen address %q: %v", args.listenAddress, err)
	}
	relayAddress := net.ParseIP(args.relayAddress)
	if relayAddress == nil {
		log.Fatalf("invalid relay address %q", args.relayAddress)
	}
	port, err := parsePort(listenPort)
	if err != nil {
		log.Fatal(err)
	}
	serverPublicKey, err := decodeServerPublicKey(args.serverPublicKey)
	if err != nil {
		log.Fatal(err)
	}
	serverID, err := peer.ServerIDFromPublicKey(serverPublicKey)
	if err != nil {
		log.Fatal(err)
	}
	allowedClients := parseClientIDs(args.clientIDs)
	if len(allowedClients) == 0 {
		log.Fatal("at least one client ID is required")
	}

	pathPrefix := strings.TrimRight(args.pathPrefix, "/")
	if pathPrefix == "" {
		pathPrefix = relayws.DefaultRelayPathPrefix
	}
	if err := validateRelayPath(pathPrefix); err != nil {
		log.Fatal(err)
	}
	relayPath, err := relayws.EndpointPath(pathPrefix, serverID)
	if err != nil {
		log.Fatal(err)
	}
	if err := validateRelayPath(relayPath); err != nil {
		log.Fatal(err)
	}
	log.Printf("server endpoint ID: %s; path namespace: %s; allowed clients: %s", serverID, relayPath, strings.Join(sortedClientIDs(allowedClients), ","))

	relay, err := relayws.NewServer(relayws.ServerConfig{
		RelayAddress: relayAddress,
		RelayPort:    port,
		CheckOrigin: func(*http.Request) bool {
			return true
		},
		Authorize: func(request relayws.AllocationRequest) error {
			if request.Path != relayPath {
				return errors.New("invalid relay path")
			}
			if request.Token != args.token {
				return errors.New("invalid relay token")
			}
			clientID, ok := requestClientID(request, serverID)
			if !ok || !allowedClients[clientID] {
				return errors.New("client endpoint is not authorized")
			}
			if request.Session != relayws.RelaySessionID(args.sessionPrefix, clientID) {
				return errors.New("invalid relay session")
			}
			return nil
		},
	})
	if err != nil {
		log.Fatalf("create websocket relay: %v", err)
	}

	router := gin.New()
	router.Use(gin.Logger())
	router.Use(gin.Recovery())
	router.GET(relayPath, gin.WrapH(relay))

	server := &http.Server{
		Addr:              args.listenAddress,
		Handler:           router,
		ReadHeaderTimeout: 10 * time.Second,
	}
	go func() {
		log.Printf("listening on %s, WebSocket endpoint ws://%s%s", server.Addr, server.Addr, relayPath)
		if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatalf("serve relay: %v", err)
		}
	}()

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)
	<-stop

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_ = relay.Close()
	if err := server.Shutdown(ctx); err != nil {
		log.Printf("shutdown relay: %v", err)
	}
}

func validateRelayPath(value string) error {
	if value == "" || value[0] != '/' {
		return errors.New("relay path must start with /")
	}
	if pathpkg.Clean(value) != value {
		return errors.New("relay path must be normalized")
	}
	if strings.ContainsAny(value, ":*") {
		return errors.New("relay path must be a static Gin path")
	}
	return nil
}

func requestClientID(request relayws.AllocationRequest, serverID string) (string, bool) {
	switch {
	case request.SourceID == serverID:
		return request.TargetID, request.TargetID != ""
	case request.TargetID == serverID:
		return request.SourceID, request.SourceID != ""
	default:
		return "", false
	}
}

func parseClientIDs(value string) map[string]bool {
	clients := make(map[string]bool)
	for _, item := range strings.Split(value, ",") {
		if clientID := strings.TrimSpace(item); clientID != "" {
			clients[clientID] = true
		}
	}
	return clients
}

func sortedClientIDs(clients map[string]bool) []string {
	result := make([]string, 0, len(clients))
	for clientID := range clients {
		result = append(result, clientID)
	}
	slices.Sort(result)
	return result
}

func decodeServerPublicKey(value string) (ed25519.PublicKey, error) {
	key, err := hex.DecodeString(strings.TrimSpace(value))
	if err != nil {
		return nil, err
	}
	if len(key) != ed25519.PublicKeySize {
		return nil, errors.New("invalid server public key length")
	}
	return ed25519.PublicKey(key), nil
}

func parsePort(value string) (uint16, error) {
	port, err := strconv.ParseUint(value, 10, 16)
	if err != nil || port == 0 {
		return 0, errors.New("invalid listen port " + value)
	}
	return uint16(port), nil
}
