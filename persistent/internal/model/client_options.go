package model

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"io/ioutil"
	"log"
	"time"

	"github.com/TykTechnologies/storage/persistent/internal/helper"
)

type ClientOpts struct {
	// ConnectionString is the expression used to connect to a storage db server.
	// It contains parameters such as username, hostname, password and port
	ConnectionString string
	// UseSSL is SSL connection is required to connect
	UseSSL bool
	// This setting allows the use of self-signed certificates when connecting to an encrypted storage database.
	SSLInsecureSkipVerify bool
	// Ignore hostname check when it differs from the original (for example with SSH tunneling).
	// The rest of the TLS verification will still be performed
	SSLAllowInvalidHostnames bool
	// Path to the PEM file with trusted root certificates
	SSLCAFile string
	// Path to the PEM file which contains both client certificate and private key. This is required for Mutual TLS.
	SSLPEMKeyfile string
	// Sets the session consistency for the storage connection
	SessionConsistency string

	// type of database/driver
	Type string
}

// GetTLSConfig returns the TLS config given the configuration specified in ClientOpts. It loads certificates if necessary.
func (opts *ClientOpts) GetTLSConfig() (*tls.Config, error) {
	tlsConfig := &tls.Config{}

	if !opts.UseSSL {
		return tlsConfig, errors.New("error getting tls config when ssl is disabled")
	}

	if opts.SSLInsecureSkipVerify {
		tlsConfig.InsecureSkipVerify = true
	}

	if opts.SSLCAFile != "" {
		caCert, err := ioutil.ReadFile(opts.SSLCAFile)
		if err != nil {
			return tlsConfig, errors.New("can't load mongo CA certificates:" + err.Error())
		}
		caCertPool := x509.NewCertPool()
		caCertPool.AppendCertsFromPEM(caCert)
		tlsConfig.RootCAs = caCertPool
	}

	if opts.SSLAllowInvalidHostnames {
		tlsConfig.InsecureSkipVerify = true
		tlsConfig.VerifyPeerCertificate = func(rawCerts [][]byte, verifiedChains [][]*x509.Certificate) error {
			// Code copy/pasted and adapted from
			// https://github.com/golang/go/blob/81555cb4f3521b53f9de4ce15f64b77cc9df61b9/src/crypto/tls/handshake_client.go#L327-L344, but adapted to skip the hostname verification.
			// See https://github.com/golang/go/issues/21971#issuecomment-412836078.

			// If this is the first handshake on a connection, process and
			// (optionally) verify the server's certificates.
			certs := make([]*x509.Certificate, len(rawCerts))
			for i, asn1Data := range rawCerts {
				cert, err := x509.ParseCertificate(asn1Data)
				if err != nil {
					return err
				}
				certs[i] = cert
			}

			opts := x509.VerifyOptions{
				Roots:         tlsConfig.RootCAs,
				CurrentTime:   time.Now(),
				DNSName:       "", // <- skip hostname verification
				Intermediates: x509.NewCertPool(),
			}

			for i, cert := range certs {
				if i == 0 {
					continue
				}
				opts.Intermediates.AddCert(cert)
			}
			_, err := certs[0].Verify(opts)

			return err
		}
	}

	if opts.SSLPEMKeyfile != "" {
		cert, err := helper.LoadCertficateAndKeyFromFile(opts.SSLPEMKeyfile)
		if err != nil {
			log.Fatal("Can't load mongo client certificate: ", err)
		}

		tlsConfig.Certificates = []tls.Certificate{*cert}
	}
	return tlsConfig, nil
}
