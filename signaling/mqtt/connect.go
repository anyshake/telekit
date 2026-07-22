package mqtt

import (
	"errors"
	"fmt"
)

func (impl *MqttAdapterImpl) Connect() error {
	if impl.client == nil {
		return errors.New("mqtt client is nil")
	}

	token := impl.client.Connect()
	if !token.WaitTimeout(impl.config.operationTimeout) {
		return fmt.Errorf("MQTT connect timed out after %s", impl.config.operationTimeout)
	}
	if err := token.Error(); err != nil {
		return fmt.Errorf("MQTT connect failed: %w", err)
	}

	return nil
}
