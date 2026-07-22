package centrifugo

import "errors"

func (impl *AdapterImpl) Connect() error {
	if impl.client == nil {
		return errors.New("client is nil")
	}
	return impl.client.Connect()
}
