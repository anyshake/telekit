package peer

import "sync"

// CallbackPool bounds asynchronous application callback concurrency and
// backlog. Submit never blocks the WebRTC receive loop.
type CallbackPool struct {
	queue  chan callbackTask
	stop   chan struct{}
	once   sync.Once
	mu     sync.Mutex
	closed bool
}

type callbackTask struct {
	run    func()
	cancel func()
}

func NewCallbackPool(workers, queueSize int) *CallbackPool {
	if workers <= 0 {
		workers = 1
	}
	if queueSize <= 0 {
		queueSize = 1
	}
	p := &CallbackPool{
		queue: make(chan callbackTask, queueSize),
		stop:  make(chan struct{}),
	}
	for range workers {
		go p.run()
	}
	return p
}

func (p *CallbackPool) run() {
	for {
		select {
		case <-p.stop:
			return
		case task := <-p.queue:
			if task.run != nil {
				task.run()
			}
		}
	}
}

func (p *CallbackPool) Submit(callback func()) bool {
	return p.SubmitWithCancel(callback, nil)
}

func (p *CallbackPool) SubmitWithCancel(callback, cancel func()) bool {
	if p == nil || callback == nil {
		return false
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return false
	}
	select {
	case p.queue <- callbackTask{run: callback, cancel: cancel}:
		return true
	default:
		return false
	}
}

func (p *CallbackPool) Close() {
	if p != nil {
		p.once.Do(func() {
			p.mu.Lock()
			p.closed = true
			close(p.stop)
			for {
				select {
				case task := <-p.queue:
					if task.cancel != nil {
						task.cancel()
					}
				default:
					p.mu.Unlock()
					return
				}
			}
		})
	}
}
