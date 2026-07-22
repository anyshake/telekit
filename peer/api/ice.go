package api

import (
	"errors"
	"fmt"
	"net/url"

	"github.com/pion/stun/v3"
	"github.com/pion/webrtc/v4"
)

type ICEServer struct {
	scheme       stun.SchemeType
	hostname     string
	port         string
	username     string
	credential   string
	transport    string
	formattedUrl string
}

func (i *ICEServer) ToPionFormat() webrtc.ICEServer {
	url := fmt.Sprintf("%s:%s:%s", i.scheme, i.hostname, i.port)
	if i.transport != "" {
		url = fmt.Sprintf("%s?transport=%s", url, i.transport)
	}
	return webrtc.ICEServer{
		URLs:           []string{url},
		Username:       i.username,
		Credential:     i.credential,
		CredentialType: webrtc.ICECredentialTypePassword,
	}
}

func newICEServer(u string) (*ICEServer, error) {
	urlObj, err := url.Parse(u)
	if err != nil {
		return nil, err
	}

	scheme := stun.NewSchemeType(urlObj.Scheme)
	if scheme == stun.SchemeTypeUnknown {
		return nil, fmt.Errorf("unknown scheme: %s", urlObj.Scheme)
	}

	hostname := urlObj.Hostname()
	if hostname == "" {
		return nil, err
	}

	port := urlObj.Port()
	if port == "" {
		switch scheme {
		case stun.SchemeTypeSTUN, stun.SchemeTypeTURN:
			port = "3478"
		case stun.SchemeTypeSTUNS, stun.SchemeTypeTURNS:
			port = "5349"
		default:
			return nil, errors.New("port is empty")
		}
	}

	transport := urlObj.Query().Get("transport")
	username := urlObj.User.Username()
	credential, _ := urlObj.User.Password()

	return &ICEServer{
		scheme:     scheme,
		hostname:   hostname,
		port:       port,
		username:   username,
		credential: credential,
		transport:  transport,
	}, nil
}
