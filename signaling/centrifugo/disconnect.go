package centrifugo

func (impl *AdapterImpl) Disconnect() error {
	if impl.client == nil {
		return nil
	}
	impl.client.Close()
	return nil
}
