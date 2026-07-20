package gcp

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// fakeTokenServer returns a local HTTP server that answers OAuth2 token requests
// so token minting runs entirely offline. It returns status for every request
// (200 with a token to simulate success, or an error status to simulate an IAM
// failure such as a missing permission) and a counter of how many requests it
// received.
func fakeTokenServer(t *testing.T, status int) (*httptest.Server, *int32) {
	t.Helper()

	var hits int32

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		atomic.AddInt32(&hits, 1)

		if status != http.StatusOK {
			w.WriteHeader(status)

			return
		}

		w.Header().Set("Content-Type", "application/json")

		if _, err := w.Write([]byte(`{"access_token":"fake-token","token_type":"Bearer","expires_in":3600}`)); err != nil {
			t.Errorf("writing token response: %v", err)
		}
	}))
	t.Cleanup(srv.Close)

	return srv, &hits
}

// writeFakeADCFile writes a syntactically valid service-account key whose
// token_uri points at tokenURI, then points GOOGLE_APPLICATION_CREDENTIALS at
// it. The RSA key is generated per-run, so no secret is committed. With a local
// tokenURI the whole flow — credential resolution and the token mint — runs
// offline without touching Google.
func writeFakeADCFile(t *testing.T, tokenURI string) {
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
		"token_uri":    tokenURI,
	}

	data, err := json.Marshal(sa)
	require.NoError(t, err)

	path := filepath.Join(t.TempDir(), "fake-adc.json")
	require.NoError(t, os.WriteFile(path, data, 0o600))

	t.Setenv("GOOGLE_APPLICATION_CREDENTIALS", path)
}

func TestBaseTokenSource_ADC_Success(t *testing.T) {
	writeFakeADCFile(t, "https://oauth2.googleapis.com/token")

	ts, err := baseTokenSource(context.Background(), "")
	require.NoError(t, err)
	assert.NotNil(t, ts)
}

func TestBaseTokenSource_Impersonation_Success(t *testing.T) {
	writeFakeADCFile(t, "https://oauth2.googleapis.com/token")

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

// TestNewCredentialsProvider_MintsTokenEagerly proves the fail-fast behavior:
// construction mints a token immediately (the local server is hit), and that
// token is cached so the returned provider hands it out without a second mint.
func TestNewCredentialsProvider_MintsTokenEagerly(t *testing.T) {
	srv, hits := fakeTokenServer(t, http.StatusOK)
	writeFakeADCFile(t, srv.URL)

	provider, err := NewCredentialsProvider(context.Background(), Config{})
	require.NoError(t, err)
	require.NotNil(t, provider)
	assert.Greater(t, atomic.LoadInt32(hits), int32(0), "NewCredentialsProvider must mint a token eagerly")

	user, pass, err := provider(context.Background())
	require.NoError(t, err)
	assert.Equal(t, "default", user)
	assert.Equal(t, "fake-token", pass)
}

func TestNewCredentialsProvider_CustomRefresh(t *testing.T) {
	srv, _ := fakeTokenServer(t, http.StatusOK)
	writeFakeADCFile(t, srv.URL)

	provider, err := NewCredentialsProvider(context.Background(), Config{RefreshBeforeExpiry: time.Minute})
	require.NoError(t, err)
	assert.NotNil(t, provider)
}

// TestNewCredentialsProvider_MintFailure proves that an IAM error surfaced while
// minting the initial token fails construction, rather than being deferred to
// the first Redis connection.
func TestNewCredentialsProvider_MintFailure(t *testing.T) {
	srv, _ := fakeTokenServer(t, http.StatusForbidden)
	writeFakeADCFile(t, srv.URL)

	_, err := NewCredentialsProvider(context.Background(), Config{})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "token")
}

func TestNewCredentialsProvider_CredentialResolutionError(t *testing.T) {
	t.Setenv("GOOGLE_APPLICATION_CREDENTIALS", filepath.Join(t.TempDir(), "does-not-exist.json"))

	_, err := NewCredentialsProvider(context.Background(), Config{})
	require.Error(t, err)
}
