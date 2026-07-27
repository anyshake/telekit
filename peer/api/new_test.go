package api

import (
	"testing"

	"github.com/pion/stun/v3"
	"github.com/stretchr/testify/require"
)

func TestParseICEURIWithTURNCredentials(t *testing.T) {
	uri, err := parseICEURI("turn://user:pass@turn.example.org:3478")
	require.NoError(t, err)
	require.Equal(t, stun.SchemeTypeTURN, uri.Scheme)
	require.Equal(t, "turn.example.org", uri.Host)
	require.Equal(t, 3478, uri.Port)
	require.Equal(t, "user", uri.Username)
	require.Equal(t, "pass", uri.Password)
	require.Equal(t, stun.ProtoTypeUDP, uri.Proto)
	require.Equal(t, "turn:turn.example.org:3478?transport=udp", uri.String())
}

func TestParseICEURIWithTURNLegacyForm(t *testing.T) {
	uri, err := parseICEURI("turn://turn.example.org:3478")
	require.NoError(t, err)
	require.Equal(t, stun.SchemeTypeTURN, uri.Scheme)
	require.Equal(t, "turn.example.org", uri.Host)
	require.Equal(t, 3478, uri.Port)
	require.Empty(t, uri.Username)
	require.Empty(t, uri.Password)
	require.Equal(t, stun.ProtoTypeUDP, uri.Proto)
}
