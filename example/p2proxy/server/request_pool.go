package main

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/anyshake/telekit/example/p2proxy/protocol"
)

type requestPool struct {
	requests  chan *protocol.Request
	done      chan struct{}
	ctx       context.Context
	cancel    context.CancelFunc
	closeOnce sync.Once
	wg        sync.WaitGroup

	resolver    *dnsResolver
	dialTimeout time.Duration
}

func newRequestPool(workers, queueSize int, resolver *dnsResolver, dialTimeout time.Duration) *requestPool {
	if workers < 1 {
		workers = 1
	}
	if queueSize < 1 {
		queueSize = workers * 8
	}
	ctx, cancel := context.WithCancel(context.Background())
	p := &requestPool{
		requests:    make(chan *protocol.Request, queueSize),
		done:        make(chan struct{}),
		ctx:         ctx,
		cancel:      cancel,
		resolver:    resolver,
		dialTimeout: dialTimeout,
	}
	p.wg.Add(workers)
	for range workers {
		go p.worker()
	}
	return p
}

func (p *requestPool) Submit(request *protocol.Request) bool {
	select {
	case p.requests <- request:
		return true
	case <-p.done:
		return false
	default:
		return false
	}
}

func (p *requestPool) worker() {
	defer p.wg.Done()
	for {
		select {
		case <-p.done:
			return
		default:
		}
		select {
		case request := <-p.requests:
			if request != nil {
				serveRequest(p.ctx, request, p.resolver, p.dialTimeout)
			}
		case <-p.done:
			return
		}
	}
}

func (p *requestPool) Close() {
	p.closeOnce.Do(func() {
		close(p.done)
		p.cancel()
		for {
			select {
			case request := <-p.requests:
				if request != nil {
					_ = request.Stream.Reject(errors.New("proxy server is shutting down"))
					_ = request.Stream.Close()
				}
			default:
				p.wg.Wait()
				return
			}
		}
	})
}
