package helper

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"testing"
)

func TestParsePrivateKey(t *testing.T) {
	t.Run("PKCS1 format", func(t *testing.T) {
		rsaKey, err := rsa.GenerateKey(rand.Reader, 2048)
		if err != nil {
			t.Fatalf("Failed to generate RSA key: %s", err)
		}
		der := x509.MarshalPKCS1PrivateKey(rsaKey)
		key, err := parsePrivateKey(der)
		if err != nil {
			t.Fatalf("Failed to parse PKCS1 private key: %s", err)
		}
		rsaKey, ok := key.(*rsa.PrivateKey)
		if !ok {
			t.Fatalf("Expected RSA private key, got %T", key)
		}
		if rsaKey.N.Cmp(rsaKey.PublicKey.N) != 0 ||
			rsaKey.E != rsaKey.PublicKey.E {
			t.Fatalf("Parsed RSA private key does not match original")
		}
	})
	t.Run("PKCS8 format", func(t *testing.T) {
		rsaKey, err := rsa.GenerateKey(rand.Reader, 2048)
		if err != nil {
			t.Fatalf("Failed to generate RSA key: %s", err)
		}
		der, err := x509.MarshalPKCS8PrivateKey(rsaKey)
		if err != nil {
			t.Fatalf("Failed to marshal PKCS8 private key: %s", err)
		}
		key, err := parsePrivateKey(der)
		if err != nil {
			t.Fatalf("Failed to parse PKCS8 private key: %s", err)
		}
		rsaKey, ok := key.(*rsa.PrivateKey)
		if !ok {
			t.Fatalf("Expected RSA private key, got %T", key)
		}
		if rsaKey.N.Cmp(rsaKey.PublicKey.N) != 0 ||
			rsaKey.E != rsaKey.PublicKey.E {
			t.Fatalf("Parsed RSA private key does not match original")
		}
	})
	t.Run("EC format", func(t *testing.T) {
		ecKey, err := ecdsa.GenerateKey(elliptic.P224(), rand.Reader)
		if err != nil {
			t.Fatalf("Failed to generate EC key: %s", err)
		}
		der, err := x509.MarshalECPrivateKey(ecKey)
		if err != nil {
			t.Fatalf("Failed to marshal EC private key: %s", err)
		}
		key, err := parsePrivateKey(der)
		if err != nil {
			t.Fatalf("Failed to parse EC private key: %s", err)
		}
		ecKey, ok := key.(*ecdsa.PrivateKey)
		if !ok {
			t.Fatalf("Expected EC private key, got %T", key)
		}
		if ecKey.X.Cmp(ecKey.PublicKey.X) != 0 ||
			ecKey.Y.Cmp(ecKey.PublicKey.Y) != 0 {
			t.Fatalf("Parsed EC private key does not match original")
		}
	})
	t.Run("Invalid format", func(t *testing.T) {
		key, err := parsePrivateKey([]byte("not a valid private key"))
		if err == nil {
			t.Fatalf("Expected error, got %v", key)
		}
	})
}
