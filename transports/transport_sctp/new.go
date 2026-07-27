package transport_sctp

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

func WithMTU(mtu uint32) Option {
	return func(t *Transport) { t.MTU = mtu }
}

func WithMaxReceiveBuffer(size uint32) Option {
	return func(t *Transport) { t.MaxReceiveBuffer = size }
}

func WithMaxMessageSize(size uint32) Option {
	return func(t *Transport) { t.MaxMessageSize = size }
}

func WithEnableInterleaving(enabled bool) Option {
	return func(t *Transport) { t.EnableInterleaving = enabled }
}

func WithBlockWrite(enabled bool) Option {
	return func(t *Transport) { t.BlockWrite = enabled }
}

func WithRTOMax(rtoMax float64) Option {
	return func(t *Transport) { t.RTOMax = rtoMax }
}

func WithMinCwnd(cwnd uint32) Option {
	return func(t *Transport) { t.MinCwnd = cwnd }
}

func WithFastRtxWnd(window uint32) Option {
	return func(t *Transport) { t.FastRtxWnd = window }
}

func WithCwndCAStep(step uint32) Option {
	return func(t *Transport) { t.CwndCAStep = step }
}
