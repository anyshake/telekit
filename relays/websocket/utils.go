package websocket

import (
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"strings"

	"github.com/gorilla/websocket"
)

// EndpointURL returns the WebSocket endpoint for one relay server namespace.
//
// If baseURL has no path, /relay/<serverID> is used. A path ending in a slash
// is treated as a custom path prefix and the server ID is appended. A path
// without a trailing slash is treated as the complete endpoint path and is
// preserved for backwards compatibility.
func EndpointURL(baseURL, serverID string) (string, error) {
	if baseURL == "" {
		return "", errors.New("websocket relay base URL is empty")
	}
	if serverID == "" {
		return "", errors.New("websocket relay server ID is empty")
	}
	u, err := url.Parse(baseURL)
	if err != nil {
		return "", fmt.Errorf("parse websocket relay base URL: %w", err)
	}
	if (u.Scheme != "ws" && u.Scheme != "wss") || u.Host == "" {
		return "", errors.New("websocket relay base URL must use ws or wss and include a host")
	}
	if u.Path == "" || u.Path == "/" {
		path, pathErr := EndpointPath("", serverID)
		if pathErr != nil {
			return "", pathErr
		}
		u.Path = path
		u.RawPath = ""
	} else if strings.HasSuffix(u.Path, "/") {
		path, pathErr := EndpointPath(u.Path, serverID)
		if pathErr != nil {
			return "", pathErr
		}
		u.Path = path
		u.RawPath = ""
	}
	return u.String(), nil
}

// EndpointPath returns the HTTP path for one relay server namespace.
//
// An empty prefix or "/" uses DefaultRelayPathPrefix. Trailing slashes are
// removed before the escaped server ID is appended.
func EndpointPath(pathPrefix, serverID string) (string, error) {
	if serverID == "" {
		return "", errors.New("websocket relay server ID is empty")
	}

	prefix := strings.TrimRight(pathPrefix, "/")
	if prefix == "" {
		prefix = DefaultRelayPathPrefix
	}
	return prefix + "/" + url.PathEscape(serverID), nil
}

func normalizeWebSocketError(err error) error {
	if err == nil {
		return nil
	}
	if websocket.IsCloseError(err, websocket.CloseNormalClosure, websocket.CloseGoingAway) ||
		errors.Is(err, net.ErrClosed) {
		return net.ErrClosed
	}
	return err
}

func closeHandshakeResponse(response *http.Response) {
	if response != nil && response.Body != nil {
		_ = response.Body.Close()
	}
}
