package main

import (
	"context"
	"fmt"
	"net"
	"sync"
	"time"
)

type dnsCacheEntry struct {
	ips     []net.IPAddr
	err     error
	expires time.Time
}

type dnsResolver struct {
	resolver *net.Resolver
	ttl      time.Duration

	mu    sync.Mutex
	cache map[string]dnsCacheEntry
}

func newDNSResolver(upstream string, timeout time.Duration) (*dnsResolver, error) {
	if _, _, err := net.SplitHostPort(upstream); err != nil {
		return nil, fmt.Errorf("invalid DNS upstream %q: %w", upstream, err)
	}
	if timeout <= 0 {
		timeout = 3 * time.Second
	}
	dialer := &net.Dialer{Timeout: timeout}
	return &dnsResolver{
		resolver: &net.Resolver{
			PreferGo: true,
			Dial: func(ctx context.Context, network, _ string) (net.Conn, error) {
				if network != "tcp" && network != "udp" {
					network = "udp"
				}
				return dialer.DialContext(ctx, network, upstream)
			},
		},
		ttl:   30 * time.Second,
		cache: make(map[string]dnsCacheEntry),
	}, nil
}

func (r *dnsResolver) lookup(ctx context.Context, host string) ([]net.IPAddr, error) {
	if ip := net.ParseIP(host); ip != nil {
		return []net.IPAddr{{IP: ip}}, nil
	}
	now := time.Now()
	r.mu.Lock()
	entry, ok := r.cache[host]
	if ok && now.Before(entry.expires) {
		ips := append([]net.IPAddr(nil), entry.ips...)
		err := entry.err
		r.mu.Unlock()
		return ips, err
	}
	r.mu.Unlock()

	ips, err := r.resolver.LookupIPAddr(ctx, host)
	expires := now.Add(r.ttl)
	if err != nil {
		expires = now.Add(5 * time.Second)
	}
	r.mu.Lock()
	r.cache[host] = dnsCacheEntry{ips: append([]net.IPAddr(nil), ips...), err: err, expires: expires}
	r.mu.Unlock()
	return ips, err
}

func (r *dnsResolver) dial(ctx context.Context, address string, timeout time.Duration) (net.Conn, error) {
	host, port, err := net.SplitHostPort(address)
	if err != nil {
		return nil, err
	}
	ips, err := r.lookup(ctx, host)
	if err != nil {
		return nil, err
	}
	dialer := &net.Dialer{Timeout: timeout}
	var lastErr error
	for _, ip := range ips {
		remote := net.JoinHostPort(ip.IP.String(), port)
		conn, dialErr := dialer.DialContext(ctx, "tcp", remote)
		if dialErr == nil {
			return conn, nil
		}
		lastErr = dialErr
	}
	if lastErr == nil {
		lastErr = fmt.Errorf("DNS returned no addresses for %q", host)
	}
	return nil, lastErr
}
