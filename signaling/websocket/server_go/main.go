package main

import (
	"log"
	"net/http"
	"os"
	"strings"

	"github.com/anyshake/telekit/signaling/websocket/broker"
	"github.com/gin-gonic/gin"
)

func main() {
	token := os.Getenv("WS_SERVER_TOKEN")
	if token == "" {
		token = "passme"
	}

	addr := os.Getenv("WS_SERVER_ADDR")
	if addr == "" {
		addr = ":8080"
	}

	router := gin.New()
	router.Use(gin.Recovery())
	router.Use(gin.Logger())

	wsBroker := broker.NewBroker(
		broker.WithOriginCheck(func(r *http.Request) bool {
			return true
		}),
		broker.WithAuthorization(func(r *http.Request, roomID string) bool {
			query := r.URL.Query()
			if !query.Has("token") || roomID == "" {
				return false
			}
			return strings.TrimSpace(query.Get("token")) == token
		}),
	)

	router.GET("/ws/:room", func(c *gin.Context) {
		room := c.Param("room")
		c.Request.URL.Path = "/" + room
		wsBroker.ServeHTTP(c.Writer, c.Request)
	})

	log.Printf("websocket broker listening on %s", addr)
	if err := router.Run(addr); err != nil {
		log.Fatal(err)
	}
}
