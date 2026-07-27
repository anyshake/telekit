package transport_rawudp

type Option func(*Transport)

const DefaultMTU = 1200

func New(options ...Option) *Transport {
	t := &Transport{MTU: DefaultMTU}
	for _, option := range options {
		if option != nil {
			option(t)
		}
	}
	return t
}

func WithMTU(mtu int) Option {
	// Raw UDP intentionally does not impose a user-space MTU.
	return func(t *Transport) { t.MTU = mtu }
}
