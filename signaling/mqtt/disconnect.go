package mqtt

import "errors"

func (impl *MqttAdapterImpl) Disconnect() error {
	if impl.client == nil {
		return errors.New("mqtt client is nil")
	}

	impl.client.Disconnect(250)
	return nil
}
