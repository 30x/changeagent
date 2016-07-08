package communication

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"io/ioutil"
	"net"
	"net/http"
	"os"
	"time"
)

/*
createVerifyingTransport creates an HTTP client transport that will verify
requests against the specifed list of CA certificates, but will not
verify anything else.
*/
func createVerifyingTransport(cfg *tls.Config, cas *x509.CertPool) *http.Transport {
	transport := http.Transport{
		Proxy:                 http.ProxyFromEnvironment,
		TLSHandshakeTimeout:   10 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
		DialTLS: func(network, addr string) (net.Conn, error) {
			return connectTLS(network, addr, cfg, cas)
		},
	}
	return &transport
}

func connectTLS(network, addr string, cfg *tls.Config, cas *x509.CertPool) (net.Conn, error) {
	conn, err := tls.Dial(network, addr, cfg)
	if err != nil {
		return nil, err
	}

	if len(conn.ConnectionState().PeerCertificates) == 0 {
		return nil, errors.New("No TLS certificate on the server side")
	}

	_, err = conn.ConnectionState().PeerCertificates[0].Verify(x509.VerifyOptions{
		Roots:     cas,
		KeyUsages: []x509.ExtKeyUsage{x509.ExtKeyUsageAny},
	})
	if err == nil {
		return conn, nil
	}
	return nil, err
}

func loadCertPool(fileName string) (*x509.CertPool, error) {
	caFile, err := os.Open(fileName)
	if err != nil {
		return nil, err
	}
	defer caFile.Close()

	pem, err := ioutil.ReadAll(caFile)
	if err != nil {
		return nil, err
	}

	pool := x509.NewCertPool()
	ok := pool.AppendCertsFromPEM(pem)
	if !ok {
		return nil, errors.New("Error loading certificates from PEM file")
	}
	return pool, nil
}
