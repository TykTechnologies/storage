package gcp

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// writeFakeADCFile writes a syntactically valid service-account key to a temp
// file and points GOOGLE_APPLICATION_CREDENTIALS at it. The RSA key is generated
// per-run, so no secret is committed. This lets the ADC and impersonation token
// sources be constructed entirely offline (a token is only fetched lazily on
// Token(), which these tests never call), exercising the wiring without touching
// Google.
func writeFakeADCFile(t *testing.T) {
	t.Helper()

	key, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)

	der, err := x509.MarshalPKCS8PrivateKey(key)
	require.NoError(t, err)

	pemKey := pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: der})

	sa := map[string]string{
		"type":         "service_account",
		"project_id":   "test-project",
		"private_key":  string(pemKey),
		"client_email": "test@test-project.iam.gserviceaccount.com",
		"client_id":    "1234567890",
		"token_uri":    "https://oauth2.googleapis.com/token",
	}

	data, err := json.Marshal(sa)
	require.NoError(t, err)

	path := filepath.Join(t.TempDir(), "fake-adc.json")
	require.NoError(t, os.WriteFile(path, data, 0o600))

	t.Setenv("GOOGLE_APPLICATION_CREDENTIALS", path)
}

func TestBaseTokenSource_ADC_Success(t *testing.T) {
	writeFakeADCFile(t)

	ts, err := baseTokenSource(context.Background(), "")
	require.NoError(t, err)
	assert.NotNil(t, ts)
}

func TestBaseTokenSource_Impersonation_Success(t *testing.T) {
	writeFakeADCFile(t)

	ts, err := baseTokenSource(context.Background(), "target@test-project.iam.gserviceaccount.com")
	require.NoError(t, err)
	assert.NotNil(t, ts)
}

func TestBaseTokenSource_ADC_Error(t *testing.T) {
	// Point at a missing credentials file so ADC resolution fails fast and
	// deterministically, without any network call.
	t.Setenv("GOOGLE_APPLICATION_CREDENTIALS", filepath.Join(t.TempDir(), "does-not-exist.json"))

	_, err := baseTokenSource(context.Background(), "")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "application default credentials")
}

func TestBaseTokenSource_Impersonation_Error(t *testing.T) {
	t.Setenv("GOOGLE_APPLICATION_CREDENTIALS", filepath.Join(t.TempDir(), "does-not-exist.json"))

	_, err := baseTokenSource(context.Background(), "target@test-project.iam.gserviceaccount.com")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "impersonation")
}

func TestNewCredentialsProvider_DefaultRefresh(t *testing.T) {
	writeFakeADCFile(t)

	provider, err := NewCredentialsProvider(context.Background(), Config{})
	require.NoError(t, err)
	assert.NotNil(t, provider)
}

func TestNewCredentialsProvider_CustomRefresh(t *testing.T) {
	writeFakeADCFile(t)

	provider, err := NewCredentialsProvider(context.Background(), Config{RefreshBeforeExpiry: time.Minute})
	require.NoError(t, err)
	assert.NotNil(t, provider)
}

func TestNewCredentialsProvider_Error(t *testing.T) {
	t.Setenv("GOOGLE_APPLICATION_CREDENTIALS", filepath.Join(t.TempDir(), "does-not-exist.json"))

	_, err := NewCredentialsProvider(context.Background(), Config{})
	require.Error(t, err)
}
