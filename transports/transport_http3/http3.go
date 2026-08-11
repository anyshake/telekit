package transport_http3

import (
	"context"
	"crypto/tls"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"strings"

	"github.com/anyshake/telekit/transports"
	transportquic "github.com/anyshake/telekit/transports/transport_quic"
	quic "github.com/apernet/quic-go"
	"github.com/apernet/quic-go/http3"
)

type quicConnContextKey struct{}

func (Transport) Name() string { return "http3" }

func (t *Transport) Dial(ctx context.Context, endpoint transports.Endpoint) (net.Conn, error) {
	if endpoint.PacketConn == nil || endpoint.RemoteAddr == nil {
		return nil, errors.New("unsupported transport")
	}
	config := t.quicConfig()
	serverName := t.serverName()
	session, err := quic.Dial(ctx, endpoint.PacketConn, endpoint.RemoteAddr, t.clientTLS(serverName), config)
	if err != nil {
		return nil, err
	}
	transportquic.InstallCongestionControl(session, congestionRemoteAddr(endpoint), config, t.bbrProfile, t.brutalBandwidth)

	requestReader, requestWriter := io.Pipe()
	requestCtx, cancelRequest := context.WithCancel(context.Background())
	path := t.path()
	request, err := http.NewRequestWithContext(requestCtx, http.MethodPost, "https://"+serverName+path, requestReader)
	if err != nil {
		cancelRequest()
		_ = session.CloseWithError(0, "request creation failed")
		return nil, err
	}
	request.ContentLength = -1
	request.Header.Set(authHeader, authToken(endpoint.AuthKey, path))
	request.Header.Set("Content-Type", "application/octet-stream")
	request.Header.Set("Cache-Control", "no-store")

	roundTripper := &http3.Transport{
		TLSClientConfig: t.clientTLS(serverName),
		QUICConfig:      config,
		Dial: func(context.Context, string, *tls.Config, *quic.Config) (*quic.Conn, error) {
			return session, nil
		},
	}
	responseCh := make(chan struct {
		response *http.Response
		err      error
	}, 1)
	go func() {
		response, roundTripErr := roundTripper.RoundTrip(request)
		responseCh <- struct {
			response *http.Response
			err      error
		}{response: response, err: roundTripErr}
	}()

	select {
	case result := <-responseCh:
		if result.err != nil {
			cancelRequest()
			_ = requestWriter.Close()
			_ = session.CloseWithError(0, "HTTP/3 request failed")
			return nil, result.err
		}
		if result.response.StatusCode != http.StatusOK || result.response.Header.Get(transportHeader) != transportValue {
			cancelRequest()
			_ = result.response.Body.Close()
			_ = requestWriter.Close()
			_ = roundTripper.Close()
			return nil, errors.New("HTTP/3 transport authentication failed")
		}
		return newConn(result.response.Body, requestWriter, session, endpoint.PacketConn, endpoint.LocalAddr, endpoint.RemoteAddr, func() {
			cancelRequest()
			_ = roundTripper.Close()
		}, func() {
			_ = requestWriter.Close()
		}), nil
	case <-ctx.Done():
		cancelRequest()
		_ = requestWriter.Close()
		_ = session.CloseWithError(0, "context canceled")
		return nil, ctx.Err()
	}
}

func (t *Transport) Accept(ctx context.Context, endpoint transports.Endpoint) (net.Conn, error) {
	if endpoint.PacketConn == nil {
		return nil, errors.New("unsupported transport")
	}
	config := t.quicConfig()
	tlsConfig, err := t.serverTLS()
	if err != nil {
		return nil, err
	}
	listener, err := quic.Listen(endpoint.PacketConn, tlsConfig, config)
	if err != nil {
		return nil, err
	}

	accepted := make(chan net.Conn, 1)
	acceptCtx, cancel := context.WithCancel(ctx)
	handler, err := t.handler(acceptCtx, endpoint, accepted, listener)
	if err != nil {
		_ = listener.Close()
		return nil, err
	}
	server := &http3.Server{
		Handler: handler,
		ConnContext: func(ctx context.Context, conn *quic.Conn) context.Context {
			transportquic.InstallCongestionControl(conn, congestionRemoteAddr(endpoint), config, t.bbrProfile, t.brutalBandwidth)
			return context.WithValue(ctx, quicConnContextKey{}, conn)
		},
	}
	// ServeListener registers the listener to generate Alt-Svc headers by
	// parsing listener.Addr(). ICE uses a logical peer address without a port,
	// so serve each QUIC connection directly and skip that public-listener
	// advertisement path.
	go func() {
		for {
			conn, acceptErr := listener.Accept(context.Background())
			if acceptErr != nil {
				return
			}
			go func(conn *quic.Conn) {
				_ = server.ServeQUICConn(conn)
			}(conn)
		}
	}()

	select {
	case conn := <-accepted:
		return conn, nil
	case <-ctx.Done():
		cancel()
		_ = listener.Close()
		return nil, ctx.Err()
	}
}

func congestionRemoteAddr(endpoint transports.Endpoint) net.Addr {
	if endpoint.Conn != nil {
		if addr := endpoint.Conn.RemoteAddr(); addr != nil {
			return addr
		}
	}
	return endpoint.RemoteAddr
}

func (t *Transport) handler(ctx context.Context, endpoint transports.Endpoint, accepted chan<- net.Conn, listener *quic.Listener) (http.Handler, error) {
	fallback, err := t.fallbackHandler()
	if err != nil {
		return nil, err
	}
	path := t.path()
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != path || !validAuthToken(endpoint.AuthKey, path, r.Header.Get(authHeader)) {
			r.Header.Del(authHeader)
			fallback.ServeHTTP(w, r)
			return
		}

		w.Header().Set(transportHeader, transportValue)
		w.Header().Set("Content-Type", "application/octet-stream")
		w.WriteHeader(http.StatusOK)
		if flusher, ok := w.(http.Flusher); ok {
			flusher.Flush()
		}
		session, _ := r.Context().Value(quicConnContextKey{}).(*quic.Conn)
		conn := newConn(r.Body, w, session, endpoint.PacketConn, endpoint.LocalAddr, endpoint.RemoteAddr, func() {
			_ = listener.Close()
		}, nil)
		select {
		case accepted <- conn:
		case <-ctx.Done():
			_ = conn.Close()
			return
		}
		<-conn.dead
	}), nil
}

func (t *Transport) fallbackHandler() (http.Handler, error) {
	if strings.TrimSpace(t.FallbackURL) == "" {
		return http.NotFoundHandler(), nil
	}
	target, err := url.Parse(t.FallbackURL)
	if err != nil || target.Scheme == "" || target.Host == "" || (target.Scheme != "http" && target.Scheme != "https") {
		return nil, errors.New("invalid HTTP/3 fallback URL")
	}
	proxy := httputil.NewSingleHostReverseProxy(target)
	director := proxy.Director
	proxy.Director = func(r *http.Request) {
		r.Header.Del(authHeader)
		director(r)
	}
	return proxy, nil
}

func (t *Transport) quicConfig() *quic.Config {
	if t.Config == nil {
		return defaultConfig()
	}
	return t.Config.Clone()
}

func (t *Transport) path() string {
	if strings.TrimSpace(t.Path) == "" {
		return defaultPath
	}
	if !strings.HasPrefix(t.Path, "/") {
		return "/" + t.Path
	}
	return t.Path
}

func (t *Transport) serverName() string {
	if t.ServerName != "" {
		return t.ServerName
	}
	if target, err := url.Parse(t.FallbackURL); err == nil && target.Hostname() != "" {
		return target.Hostname()
	}
	return "www.microsoft.com"
}
