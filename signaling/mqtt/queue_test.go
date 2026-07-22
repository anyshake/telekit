package mqtt

import "testing"

func TestDispatchQueueLimits(t *testing.T) {
	overflow := 0
	impl := &MqttAdapterImpl{
		config: &config{
			dispatchQueueMessages: 2,
			dispatchQueueBytes:    5,
			onDispatchOverflow:    func(string) { overflow++ },
		},
		subs:        make(map[string]*topicSubscriptions),
		dispatching: true, // Keep the test deterministic; do not start the drainer.
	}
	impl.enqueueDispatch("a", []byte("12"))
	impl.enqueueDispatch("a", []byte("345"))
	impl.enqueueDispatch("a", []byte("6"))
	if len(impl.dispatchQueue) != 2 || impl.dispatchBytes != 5 {
		t.Fatalf("queue = %d messages/%d bytes", len(impl.dispatchQueue), impl.dispatchBytes)
	}
	if overflow != 1 {
		t.Fatalf("overflow callback count = %d", overflow)
	}
}
