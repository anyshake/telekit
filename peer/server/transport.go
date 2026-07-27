package server

import (
	"github.com/anyshake/telekit/transports"
)

func findTransport(available []transports.ITransport, wanted string) transports.ITransport {
	for _, transport := range available {
		if transport != nil && transport.Name() == wanted {
			return transport
		}
	}
	return nil
}

func containsTransport(available []transports.ITransport, wanted string) bool {
	return findTransport(available, wanted) != nil
}

func transportNames(available []transports.ITransport) []string {
	names := make([]string, 0, len(available))
	for _, transport := range available {
		if transport != nil {
			names = append(names, transport.Name())
		}
	}
	return names
}
