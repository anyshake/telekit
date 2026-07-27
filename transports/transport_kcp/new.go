package transport_kcp

type Option func(*Transport)

func New(options ...Option) *Transport {
	t := DefaultTransport()
	for _, option := range options {
		if option != nil {
			option(&t)
		}
	}
	return &t
}

func WithDataShards(shards int) Option {
	return func(t *Transport) { t.DataShards = shards }
}

func WithMTU(mtu int) Option {
	return func(t *Transport) { t.MTU = mtu }
}

func WithParityShards(shards int) Option {
	return func(t *Transport) { t.ParityShards = shards }
}

func WithFEC(dataShards, parityShards int) Option {
	return func(t *Transport) {
		t.DataShards = dataShards
		t.ParityShards = parityShards
	}
}

func WithAdaptiveCongestionControl(enabled bool) Option {
	return func(t *Transport) { t.AdaptiveCongestionControl = enabled }
}

func WithCongestionControl(enabled bool) Option {
	return func(t *Transport) { t.DisableCongestionControl = !enabled }
}
