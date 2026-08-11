package transport_http3

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
)

const (
	authHeader      = "X-Telekit-Auth"
	transportHeader = "X-Telekit-Transport"
	transportValue  = "h3-v1"
)

func authToken(key []byte, path string) string {
	h := hmac.New(sha256.New, key)
	_, _ = h.Write([]byte("telekit/http3/v1\nPOST\n"))
	_, _ = h.Write([]byte(path))
	return base64.RawURLEncoding.EncodeToString(h.Sum(nil))
}

func validAuthToken(key []byte, path, token string) bool {
	if len(key) == 0 || token == "" {
		return false
	}
	expected := authToken(key, path)
	return hmac.Equal([]byte(expected), []byte(token))
}
