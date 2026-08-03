package transport_quic

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"net"
	"time"

	"github.com/anyshake/telekit/transports/transport_quic/congestion/bbr"
	"github.com/anyshake/telekit/transports/transport_quic/congestion/brutal"
	quic "github.com/apernet/quic-go"
	"github.com/apernet/quic-go/congestion"
)

func installCongestionControl(session *quic.Conn, remote net.Addr, config *quic.Config, profile bbr.Profile, brutalBandwidth uint64) {
	if session == nil {
		return
	}
	if brutalBandwidth > 0 {
		session.SetCongestionControl(brutal.NewSender(brutalBandwidth))
		return
	}
	initialPacketSize := bbr.GetInitialPacketSize(remote)
	if config != nil && config.InitialPacketSize > 0 {
		// Keep the controller's packet-sized windows consistent with quic-go's
		// configured initial packet size. This matters when the PacketConn is an
		// ICE adapter and its public address is not a *net.UDPAddr.
		initialPacketSize = congestion.ByteCount(config.InitialPacketSize)
	}
	// Hysteria/Xray's BBR profiles are designed for high RTT and lossy paths.
	// They use delivery-rate sampling plus pacing instead of quic-go's stock
	// loss-driven controller.
	session.SetCongestionControl(bbr.NewBbrSender(
		bbr.DefaultClock{},
		initialPacketSize,
		profile,
	))
}

func clientTLS() *tls.Config {
	return &tls.Config{InsecureSkipVerify: true, NextProtos: []string{protocolName}}
}

func serverTLS() *tls.Config {
	certificate, err := certificate()
	if err != nil {
		return &tls.Config{NextProtos: []string{protocolName}}
	}
	return &tls.Config{Certificates: []tls.Certificate{certificate}, NextProtos: []string{protocolName}}
}

func certificate() (tls.Certificate, error) {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return tls.Certificate{}, err
	}
	serial, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 128))
	if err != nil {
		return tls.Certificate{}, err
	}
	template := &x509.Certificate{SerialNumber: serial, Subject: pkix.Name{CommonName: "telekit"}, NotBefore: time.Now().Add(-time.Minute), NotAfter: time.Now().Add(24 * time.Hour), KeyUsage: x509.KeyUsageKeyEncipherment | x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		return tls.Certificate{}, err
	}
	keyDER, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		return tls.Certificate{}, err
	}
	return tls.X509KeyPair(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: keyDER}))
}
