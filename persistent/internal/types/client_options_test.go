package types

import (
	"crypto/tls"
	"testing"
)

func TestVerifyPeerCertificate(t *testing.T) {
	tlsConfig := &tls.Config{}
	opts := &ClientOpts{}

	opts.verifyPeerCertificate(tlsConfig)

	if tlsConfig.VerifyPeerCertificate == nil {
		t.Error("Expected VerifyPeerCertificate to be set, but it is nil")
	}
}
