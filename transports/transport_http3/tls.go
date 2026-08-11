package transport_http3

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"time"

	"github.com/apernet/quic-go/http3"
)

func (t *Transport) clientTLS(serverName string) *tls.Config {
	var config *tls.Config
	if t.TLSConfig != nil {
		config = t.TLSConfig.Clone()
	} else {
		config = &tls.Config{}
	}
	config.InsecureSkipVerify = true //nolint:gosec // peer authentication is performed before ICE
	config.ServerName = serverName
	config.NextProtos = []string{http3.NextProtoH3}
	return config
}

func (t *Transport) serverTLS() (*tls.Config, error) {
	if t.TLSConfig != nil {
		return http3.ConfigureTLSConfig(t.TLSConfig), nil
	}
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, err
	}
	serial, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 128))
	if err != nil {
		return nil, err
	}
	template := &x509.Certificate{
		SerialNumber: serial,
		Subject:      pkix.Name{CommonName: "telekit-http3"},
		NotBefore:    time.Now().Add(-time.Minute),
		NotAfter:     time.Now().Add(24 * time.Hour),
		KeyUsage:     x509.KeyUsageKeyEncipherment | x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		return nil, err
	}
	keyDER, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		return nil, err
	}
	certificate, err := tls.X509KeyPair(
		pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}),
		pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: keyDER}),
	)
	if err != nil {
		return nil, err
	}
	return http3.ConfigureTLSConfig(&tls.Config{Certificates: []tls.Certificate{certificate}}), nil
}
