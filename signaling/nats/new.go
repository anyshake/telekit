package nats

import (
	"errors"
	"time"

	"github.com/anyshake/telekit/signaling"
	natsgo "github.com/nats-io/nats.go"
)

func NewAdapter(url string, opts ...natsgo.Option) (signaling.Adapter, error) {
	return NewAdapterWithBaseSubject(url, DefaultBaseSubject, opts...)
}

// NewAdapterWithBaseSubject creates an adapter whose subjects are rooted at
// baseSubject. For example, "sensors.telekit" maps to
// sensors.telekit.<room>.<type>. Both peers must use the same value.
func NewAdapterWithBaseSubject(url, baseSubject string, opts ...natsgo.Option) (signaling.Adapter, error) {
	if url == "" {
		return nil, errors.New("NATS URL is required")
	}
	if err := signaling.ValidateRoutePrefix(baseSubject, '.'); err != nil {
		return nil, err
	}
	reconnectOpts := make([]natsgo.Option, 0, len(opts)+1)
	reconnectOpts = append(reconnectOpts, natsgo.MaxReconnects(-1))
	reconnectOpts = append(reconnectOpts, opts...)
	return &Adapter{url: url, baseSubject: baseSubject, opts: reconnectOpts}, nil
}

// WithReconnectBackoff configures exponential reconnect delays for NATS.
// NATS will continue reconnecting until MaxReconnects is reached.
func WithReconnectBackoff(minDelay, maxDelay time.Duration) natsgo.Option {
	return func(options *natsgo.Options) error {
		if minDelay <= 0 || maxDelay <= 0 || maxDelay < minDelay {
			return errors.New("NATS reconnect backoff must be positive and max >= min")
		}
		return natsgo.CustomReconnectDelay(func(attempt int) time.Duration {
			delay := minDelay
			for i := 1; i < attempt && delay < maxDelay; i++ {
				if delay > maxDelay/2 {
					return maxDelay
				}
				delay *= 2
			}
			if delay > maxDelay {
				return maxDelay
			}
			return delay
		})(options)
	}
}

// WithMaxReconnects configures how many reconnect attempts NATS makes. Use a
// negative value for unlimited attempts.
func WithMaxReconnects(max int) natsgo.Option {
	return natsgo.MaxReconnects(max)
}

func WithOnConnect(handler func()) natsgo.Option {
	return natsgo.ConnectHandler(func(*natsgo.Conn) {
		if handler != nil {
			handler()
		}
	})
}

func WithConnectionLostHandler(handler func(error)) natsgo.Option {
	return natsgo.DisconnectErrHandler(func(_ *natsgo.Conn, err error) {
		if handler != nil {
			handler(err)
		}
	})
}

func WithReconnectingHandler(handler func()) natsgo.Option {
	return natsgo.ReconnectHandler(func(*natsgo.Conn) {
		if handler != nil {
			handler()
		}
	})
}

func WithReconnectErrorHandler(handler func(error)) natsgo.Option {
	return natsgo.ReconnectErrHandler(func(_ *natsgo.Conn, err error) {
		if handler != nil {
			handler(err)
		}
	})
}
