package gcp

import (
	"context"
	"encoding/json"
	"time"

	secretmanager "cloud.google.com/go/secretmanager/apiv1"
	"github.com/TykTechnologies/storage/kv"
	"google.golang.org/api/option"
)

// Config is the JSON "config" block of a gcp_secret_manager store.
type Config struct {
	// ProjectID is the GCP project that owns the secrets. Required.
	ProjectID string `json:"project_id"`

	// QuotaProjectID sets the project billed/quota-attributed for API calls
	// (the x-goog-user-project header, via option.WithQuotaProject). Optional.
	QuotaProjectID string `json:"quota_project_id"`

	// Location, when set, targets REGIONAL Secret Manager (data residency).
	// Example: "europe-west1". When set, the client uses the regional endpoint
	// and all resource names include /locations/<Location>/. Optional.
	Location string `json:"location"`

	// CredentialsType describes how to interpret an EXPLICIT credential passed via
	// CredentialsFile/CredentialsJSON. It does NOT select an auth method.
	//   - ADC path (no CredentialsFile/JSON): the type is auto-detected from the ADC
	//     source (GOOGLE_APPLICATION_CREDENTIALS file, gcloud, or metadata server), so
	//     this field is irrelevant and must be empty. WIF works this way too — point
	//     GOOGLE_APPLICATION_CREDENTIALS at the WIF file and leave this empty.
	//   - Explicit path: set CredentialsFile or CredentialsJSON AND this field to one of
	//     "service_account" | "authorized_user" | "external_account".
	//
	// Setting it without any credential is a no-op mistake, so it is rejected.
	CredentialsType string `json:"credentials_type"`

	// CredentialsFile is a path to a credentials JSON file. Mutually exclusive
	// with CredentialsJSON. Optional.
	CredentialsFile string `json:"credentials_file"`

	// CredentialsJSON is the raw credentials JSON payload (e.g. injected via env).
	// Mutually exclusive with CredentialsFile. Optional.
	CredentialsJSON string `json:"credentials_json"`

	// ImpersonateServiceAccount, when set, makes the client fetch/write secrets AS
	// this target SA. The base identity is the explicit credentials above if
	// provided, otherwise ADC. The base identity needs
	// roles/iam.serviceAccountTokenCreator on the target. Optional.
	ImpersonateServiceAccount string `json:"impersonate_service_account"`

	// ImpersonateDelegates is chained-delegation path to reach the target
	// principal. Meaningful only with ImpersonateServiceAccount. Optional.
	ImpersonateDelegates []string `json:"impersonate_delegates"`

	// Timeout bounds each RPC. Go duration string ("5s", "500ms"). Optional.
	Timeout string `json:"timeout"`

	// TrimTrailingNewline, when true, strips a single trailing "\n" from the
	// payload. Guards the common footgun of secrets created with a trailing
	// newline (e.g. `echo val | gcloud secrets create`). Optional.
	TrimTrailingNewline bool `json:"trim_trailing_newline"`

	// Transport selects the wire protocol: "grpc" (default, recommended) or
	// "rest". Use "rest" only in networks hostile to gRPC/HTTP-2. Optional.
	Transport string `json:"transport"`
}

func NewFactory() kv.ProviderFactory {
	return func(config json.RawMessage) (kv.Provider, error) {
		return nil, nil
	}
}

var (
	_ kv.Provider    = (*gcpProvider)(nil)
	_ kv.Setter      = (*gcpProvider)(nil)
	_ kv.Initializer = (*gcpProvider)(nil)
	_ kv.Timeouter   = (*gcpProvider)(nil)
	_ kv.Closer      = (*gcpProvider)(nil)
)

type gcpProvider struct {
	cfg     *Config
	client  *secretmanager.Client
	timeout time.Duration
	// INFO: Not sure if I need testOpts field at all.
	testOpts []option.ClientOption
}

func (gp *gcpProvider) Get(ctx context.Context, key string) (string, error) {
	return "", nil
}

func (gp *gcpProvider) projectParent() string         { return "" }
func (gp *gcpProvider) secretName(id string) string   { return "" }
func (gp *gcpProvider) versionName(key string) string { return "" }

func (gp *gcpProvider) Set(ctx context.Context, key, value string) error {
	return nil
}

func (gp *gcpProvider) Timeout() time.Duration {
	return gp.timeout
}

func (gp *gcpProvider) Init(ctx context.Context) error {
	return nil
}

func (gp *gcpProvider) Close(ctx context.Context) error {
	return nil
}
