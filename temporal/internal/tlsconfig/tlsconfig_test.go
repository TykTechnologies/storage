package tlsconfig

import (
	"crypto/tls"
	"errors"
	"os"
	"testing"

	"github.com/TykTechnologies/storage/temporal/model"
	"github.com/TykTechnologies/storage/temporal/temperr"
)

func TestHandleTLS(t *testing.T) {
	tests := []struct {
		name        string
		cfg         *model.TLS
		expectedErr error
	}{
		{
			name: "Valid config with Cert and Key",
			cfg: &model.TLS{
				Enable:             true,
				InsecureSkipVerify: false,
				CertFile:           os.Getenv("TEST_TLS_CERT_FILE"),
				KeyFile:            os.Getenv("TEST_TLS_KEY_FILE"),
				CAFile:             os.Getenv("TEST_TLS_CA_FILE"),
				MaxVersion:         "1.3",
				MinVersion:         "1.2",
			},
		},
		{
			name: "Invalid Cert and Key paths",
			cfg: &model.TLS{
				Enable:             true,
				InsecureSkipVerify: false,
				CertFile:           "invalid/certfile",
				KeyFile:            "/keyfile",
				CAFile:             "",
				MaxVersion:         "1.3",
				MinVersion:         "1.2",
			},
			expectedErr: errors.New("open invalid/certfile: no such file or directory"),
		},
		{
			name: "Invalid TLS version",
			cfg: &model.TLS{
				Enable:             true,
				InsecureSkipVerify: false,
				CertFile:           os.Getenv("TEST_TLS_CERT_FILE"),
				KeyFile:            os.Getenv("TEST_TLS_KEY_FILE"),
				CAFile:             os.Getenv("TEST_TLS_CA_FILE"),
				MaxVersion:         "1.4",
				MinVersion:         "1.2",
			},
			expectedErr: temperr.InvalidTLSMaxVersion,
		},
		{
			name: "Invalid CA file",
			cfg: &model.TLS{
				Enable:             true,
				InsecureSkipVerify: false,
				CertFile:           os.Getenv("TEST_TLS_CERT_FILE"),
				KeyFile:            os.Getenv("TEST_TLS_KEY_FILE"),
				CAFile:             "invalid/cafile",
				MaxVersion:         "1.3",
				MinVersion:         "1.2",
			},
			expectedErr: errors.New("open invalid/cafile: no such file or directory"),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := HandleTLS(tt.cfg)
			if err != nil {
				if tt.expectedErr == nil {
					t.Errorf("HandleTLS() error = %v, expectedErr %v", err, tt.expectedErr)
				} else if err.Error() != tt.expectedErr.Error() {
					t.Errorf("HandleTLS() error = %v, expectedErr %v", err, tt.expectedErr)
				}
			}

			if err == nil && tt.expectedErr != nil {
				t.Errorf("HandleTLS() error = %v, expectedErr %v", err, tt.expectedErr)
			}
		})
	}
}

func TestHandleTLSVersion(t *testing.T) {
	tests := []struct {
		name           string
		cfg            *model.TLS
		wantMinVersion int
		wantMaxVersion int
		wantErr        error
	}{
		{
			name: "Valid version range",
			cfg: &model.TLS{
				MinVersion: "1.2",
				MaxVersion: "1.3",
			},
			wantMinVersion: tls.VersionTLS12,
			wantMaxVersion: tls.VersionTLS13,
			wantErr:        nil,
		},
		{
			name: "Invalid max version",
			cfg: &model.TLS{
				MinVersion: "1.2",
				MaxVersion: "1.4", // invalid version
			},
			wantMinVersion: 0,
			wantMaxVersion: 0,
			wantErr:        temperr.InvalidTLSMaxVersion,
		},
		{
			name: "Invalid min version",
			cfg: &model.TLS{
				MinVersion: "1.4", // invalid version
				MaxVersion: "1.3",
			},
			wantMinVersion: 0,
			wantMaxVersion: tls.VersionTLS13,
			wantErr:        temperr.InvalidTLSMinVersion,
		},
		{
			name: "Default values",
			cfg: &model.TLS{
				MinVersion: "",
				MaxVersion: "",
			},
			wantMinVersion: tls.VersionTLS12,
			wantMaxVersion: tls.VersionTLS13,
			wantErr:        nil,
		},
		{
			name: "Min version higher than max version",
			cfg: &model.TLS{
				MinVersion: "1.3",
				MaxVersion: "1.2",
			},
			wantMinVersion: tls.VersionTLS13,
			wantMaxVersion: tls.VersionTLS12,
			wantErr:        temperr.InvalidTLSVersion,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			minVersion, maxVersion, err := HandleTLSVersion(tt.cfg)

			if (err != nil) != (tt.wantErr != nil) {
				t.Errorf("HandleTLSVersion() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			if tt.wantErr != nil {
				if err == nil {
					t.Errorf("HandleTLSVersion() error = %v, wantErr %v", err, tt.wantErr)
					return
				}

				if err.Error() != tt.wantErr.Error() {
					t.Errorf("HandleTLSVersion() error = %v, wantErr %v", err, tt.wantErr)
				}
			}

			if minVersion != tt.wantMinVersion {
				t.Errorf("HandleTLSVersion() minVersion = %v, wantMinVersion %v", minVersion, tt.wantMinVersion)
			}

			if maxVersion != tt.wantMaxVersion {
				t.Errorf("HandleTLSVersion() maxVersion = %v, wantMaxVersion %v", maxVersion, tt.wantMaxVersion)
			}
		})
	}
}
