package nats

func (a *Adapter) Disconnect() error {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.nc == nil {
		return nil
	}
	a.nc.Close()
	a.nc = nil
	return nil
}
