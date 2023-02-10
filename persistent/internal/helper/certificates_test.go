package helper

import (
	"bytes"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"fmt"
	"io/ioutil"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestParsePrivateKey(t *testing.T) {
	t.Run("PKCS1 format", func(t *testing.T) {
		rsaKey, err := rsa.GenerateKey(rand.Reader, 2048)
		assert.Nil(t, err)

		der := x509.MarshalPKCS1PrivateKey(rsaKey)
		key, err := parsePrivateKey(der)
		assert.Nil(t, err)
		rsaKey, ok := key.(*rsa.PrivateKey)
		assert.True(t, ok)

		validation := rsaKey.N.Cmp(rsaKey.PublicKey.N) != 0 ||
			rsaKey.E != rsaKey.PublicKey.E
		assert.False(t, validation)
	})
	t.Run("PKCS8 format", func(t *testing.T) {
		rsaKey, err := rsa.GenerateKey(rand.Reader, 2048)
		assert.Nil(t, err)

		der, err := x509.MarshalPKCS8PrivateKey(rsaKey)
		assert.Nil(t, err)

		key, err := parsePrivateKey(der)
		assert.Nil(t, err)

		rsaKey, ok := key.(*rsa.PrivateKey)
		assert.True(t, ok)

		validation := rsaKey.N.Cmp(rsaKey.PublicKey.N) != 0 ||
			rsaKey.E != rsaKey.PublicKey.E
		assert.False(t, validation)
	})
	t.Run("EC format", func(t *testing.T) {
		ecKey, err := ecdsa.GenerateKey(elliptic.P224(), rand.Reader)
		assert.Nil(t, err)

		der, err := x509.MarshalECPrivateKey(ecKey)
		assert.Nil(t, err)

		key, err := parsePrivateKey(der)
		assert.Nil(t, err)

		ecKey, ok := key.(*ecdsa.PrivateKey)
		assert.True(t, ok)

		validation := ecKey.X.Cmp(ecKey.PublicKey.X) != 0 ||
			ecKey.Y.Cmp(ecKey.PublicKey.Y) != 0
		assert.False(t, validation)
	})
	t.Run("Invalid format", func(t *testing.T) {
		_, err := parsePrivateKey([]byte("not a valid private key"))
		assert.NotNil(t, err)
	})
}

func TestLoadCertificateAndKey(t *testing.T) {
	certPEM, keyPEM, _, _ := GenServerCertificate(t)

	tests := []struct {
		name    string
		certPEM []byte
		keyPEM  []byte
		wantErr error
	}{
		{
			name:    "valid certificate and key",
			certPEM: certPEM,
			keyPEM:  keyPEM,
			wantErr: nil,
		},
		{
			name:    "empty certificate",
			certPEM: []byte{},
			keyPEM:  keyPEM,
			wantErr: fmt.Errorf("no certificate found"),
		},
		{
			name:    "empty key",
			certPEM: certPEM,
			keyPEM:  []byte{},
			wantErr: fmt.Errorf("no private key found"),
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := LoadCertificateAndKey(append(test.certPEM, test.keyPEM...))
			assert.Equal(t, test.wantErr, err)
		})
	}
}

func TestLoadCertificateAndKeyFromFile(t *testing.T) {
	_, _, combinedPEM, _ := GenServerCertificate(t)
	dir, err := ioutil.TempDir("", "certs")
	defer os.RemoveAll(dir)

	assert.Nil(t, err)


	certCombinedPath := filepath.Join(dir, "combined.pem")
	err = ioutil.WriteFile(certCombinedPath, combinedPEM, 0o666)
	assert.Nil(t, err)

	tcs := []struct {
		name        string
		file        string
		expectedErr error
	}{
		{
			name:        "valid certificate and key file",
			file:        certCombinedPath,
			expectedErr: nil,
		},
		{
			name:        "invalid file",
			file:        "random.pem",
			expectedErr: errors.New("failure reading certificate file: open random.pem: no such file or directory"),
		},
	}

	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			cert, err := LoadCertificateAndKeyFromFile(tc.file)
			assert.Equal(t, tc.expectedErr, err)
			if err == nil {
				assert.NotNil(t, cert)
			}
		})
	}
}

func GenCertificate(t *testing.T, template *x509.Certificate, setLeaf bool) ([]byte, []byte, []byte, tls.Certificate) {
	t.Helper()
	priv, _ := rsa.GenerateKey(rand.Reader, 1024)

	serialNumberLimit := new(big.Int).Lsh(big.NewInt(1), 128)
	serialNumber, _ := rand.Int(rand.Reader, serialNumberLimit)
	template.SerialNumber = serialNumber
	template.BasicConstraintsValid = true
	template.NotBefore = time.Now()
	template.NotAfter = template.NotBefore.Add(time.Hour)

	derBytes, _ := x509.CreateCertificate(rand.Reader, template, template, &priv.PublicKey, priv)

	var certPem, keyPem bytes.Buffer
	pem.Encode(&certPem, &pem.Block{Type: "CERTIFICATE", Bytes: derBytes})
	pem.Encode(&keyPem, &pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(priv)})

	clientCert, _ := tls.X509KeyPair(certPem.Bytes(), keyPem.Bytes())
	if setLeaf {
		clientCert.Leaf = template
	}
	combinedPEM := bytes.Join([][]byte{certPem.Bytes(), keyPem.Bytes()}, []byte("\n"))

	return certPem.Bytes(), keyPem.Bytes(), combinedPEM, clientCert
}

func GenServerCertificate(t *testing.T) ([]byte, []byte, []byte, tls.Certificate) {
	t.Helper()
	certPem, privPem, combinedPEM, cert := GenCertificate(t, &x509.Certificate{
		DNSNames:    []string{"localhost"},
		IPAddresses: []net.IP{net.ParseIP("127.0.0.1"), net.ParseIP("::")},
	}, false)

	return certPem, privPem, combinedPEM, cert
}
