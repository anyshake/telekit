package nats

import natsgo "github.com/nats-io/nats.go"

func (a *Adapter) Connect() error {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.nc != nil && !a.nc.IsClosed() {
		return nil
	}
	nc, err := natsgo.Connect(a.url, a.opts...)
	if err != nil {
		return err
	}
	a.nc = nc
	return nil
}
