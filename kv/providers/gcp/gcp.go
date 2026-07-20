package gcp

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"slices"
	"strings"
	"time"

	secretmanager "cloud.google.com/go/secretmanager/apiv1"
	secretmanagerpb "cloud.google.com/go/secretmanager/apiv1/secretmanagerpb"
	"github.com/TykTechnologies/storage/kv"
	"google.golang.org/api/impersonate"
	"google.golang.org/api/option"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const versionsPath = "/versions/"

var castagnoli = crc32.MakeTable(crc32.Castagnoli)

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

var (
	// TODO: the option package exposes  type CredentialsType = credentialstype.CredType, mb I should use this type?
	// Why yes/ why not?
	allowedCredentialsTypes = []string{"service_account", "authorized_user", "external_account"}
	allowedTransports       = []string{"grpc", "rest"}
)

func NewFactory() kv.ProviderFactory {
	return func(raw json.RawMessage) (kv.Provider, error) {
		if len(raw) == 0 {
			return nil, errors.New("gcp: config is missing")
		}

		var config Config
		if err := json.Unmarshal(raw, &config); err != nil {
			return nil, fmt.Errorf("gcp: invalid config: %w", err)
		}

		if config.ProjectID == "" {
			return nil, errors.New("gcp: project_id is required")
		}

		hasFile := config.CredentialsFile != ""
		hasJSON := config.CredentialsJSON != ""

		if hasFile && hasJSON {
			return nil, errors.New("gcp: credentials_file and credentials_json are mutually exclusive")
		}

		if hasFile || hasJSON {
			if config.CredentialsType == "" {
				return nil, errors.New("gcp: credentials_type is required with credentials_file/credentials_json")
			}

			if !slices.Contains(allowedCredentialsTypes, config.CredentialsType) {
				return nil, fmt.Errorf("gcp: unsupported or insecure credentials_type: %q", config.CredentialsType)
			}

			if config.CredentialsType == "external_account" {
				err := validateExternalAccount(&config)
				if err != nil {
					return nil, err
				}
			}
		} else if config.CredentialsType != "" {
			return nil, errors.New("gcp: credentials_type set without credentials_file or credentials_json")
		}

		if len(config.ImpersonateDelegates) > 0 && config.ImpersonateServiceAccount == "" {
			return nil, errors.New("gcp: impersonate_delegates set without impersonate_service_account")
		}

		if config.Transport != "" && !slices.Contains(allowedTransports, config.Transport) {
			return nil, fmt.Errorf(`gcp: unsupported transport %q (want "grpc" or "rest")`, config.Transport)
		}

		var timeout time.Duration

		if config.Timeout != "" {
			d, err := time.ParseDuration(config.Timeout)
			if err != nil {
				return nil, fmt.Errorf("gcp: invalid timeout %q: %w", config.Timeout, err)
			}

			if d > 0 {
				timeout = d
			}
		}

		return &gcpProvider{
			cfg:     &config,
			timeout: timeout,
		}, nil
	}
}

// TODO: Add implementation with using constraints from:
// https://docs.cloud.google.com/docs/authentication/client-libraries#validate_other_credential_configurations
func validateExternalAccount(config *Config) error {
	return nil
}

var (
	_ kv.Provider    = (*gcpProvider)(nil)
	_ kv.Setter      = (*gcpProvider)(nil)
	_ kv.Initializer = (*gcpProvider)(nil)
	_ kv.Timeouter   = (*gcpProvider)(nil)
	_ kv.Closer      = (*gcpProvider)(nil)
)

type gcpProvider struct {
	cfg      *Config
	client   *secretmanager.Client
	timeout  time.Duration
	testOpts []option.ClientOption
}

// INFO: QUESTIONS:
// 1. Do we have to expose custom scopes passing for client? It looks like
// we need scopes for impersonation, the question is if we want to hardcode it
// or expose to client. Why yes/ why not?
// 2.
func (gp *gcpProvider) Init(ctx context.Context) error {
	var opts []option.ClientOption

	if len(gp.testOpts) > 0 {
		opts = gp.testOpts
	} else {
		// TODO: Before all of this we have to process with ADC. Or we don't have to do anything?

		if gp.cfg.CredentialsFile != "" {
			opts = append(opts, option.WithAuthCredentialsFile(option.CredentialsType(gp.cfg.CredentialsType), gp.cfg.CredentialsFile))
		}

		if gp.cfg.CredentialsJSON != "" {
			opts = append(opts, option.WithAuthCredentialsJSON(option.CredentialsType(gp.cfg.CredentialsType), []byte(gp.cfg.CredentialsJSON)))
		}

		// TODO: Finish the impersonation
		if gp.cfg.ImpersonateServiceAccount != "" {
			cs := impersonate.CredentialsConfig{
				TargetPrincipal: gp.cfg.ImpersonateServiceAccount,
				Delegates:       gp.cfg.ImpersonateDelegates,
				// FIX: Scopes are required. What are they?
				// Scopes:
			}
			// TODO: Do I need to pass any Client Options here?
			impersonateCTS, err := impersonate.CredentialsTokenSource(ctx, cs)
			if err != nil {
				return fmt.Errorf("gcp: create credentials token source: %w", err)
			}

			opts = append(opts, option.WithTokenSource(impersonateCTS))
		}

		if gp.cfg.QuotaProjectID != "" {
			opts = append(opts, option.WithQuotaProject(gp.cfg.QuotaProjectID))
		}
	}

	newClient := secretmanager.NewClient
	if gp.cfg.Transport == "rest" {
		newClient = secretmanager.NewRESTClient
	}

	c, err := newClient(ctx, opts...)
	if err != nil {
		return fmt.Errorf("gcp: create client: %w", err)
	}

	gp.client = c

	return nil
}

func (gp *gcpProvider) Get(ctx context.Context, key string) (string, error) {
	err := gp.validateSecretKey(key)
	if err != nil {
		return "", err
	}

	req := &secretmanagerpb.AccessSecretVersionRequest{
		Name: gp.versionName(key),
	}

	resp, err := gp.client.AccessSecretVersion(ctx, req)
	if err != nil {
		return "", gp.classify(key, err)
	}

	payload := resp.GetPayload()
	if payload == nil {
		return "", &kv.StoreUnavailableError{KeyPath: key, Err: errors.New("gcp: nil payload")}
	}

	data := payload.GetData()

	if payload.DataCrc32C != nil {
		requestCrc32C := crc32.Checksum(data, castagnoli)

		if int64(requestCrc32C) != payload.GetDataCrc32C() {
			return "", &kv.StoreUnavailableError{KeyPath: key, Err: errors.New("gcp: payload checksum mismatch")}
		}
	}

	out := string(data)

	if gp.cfg.TrimTrailingNewline {
		out = strings.TrimSuffix(out, "\n")
	}

	return out, nil
}

func (gp *gcpProvider) validateSecretKey(key string) error {
	id := key

	if i := strings.Index(id, versionsPath); i >= 0 {
		id = id[:i]
	}

	if id == "" {
		return errors.New("gcp: empty secret key")
	}

	if strings.Contains(id, "/") {
		return fmt.Errorf("gcp: secret key %q must be a bare secret ID; "+
			"cross-project/full resource names are not allowed (configure a separate store per project)", key)
	}

	return nil
}

func (gp *gcpProvider) versionName(key string) string {
	base := gp.secretName(key)

	if strings.Contains(base, versionsPath) {
		return base
	}

	return base + versionsPath + "latest"
}

func (gp *gcpProvider) secretName(id string) string {
	return gp.projectParent() + "/secrets/" + id
}

func (gp *gcpProvider) projectParent() string {
	if gp.cfg.Location != "" {
		return fmt.Sprintf("projects/%s/locations/%s", gp.cfg.ProjectID, gp.cfg.Location)
	}

	return "projects/" + gp.cfg.ProjectID
}

func (gp *gcpProvider) classify(key string, err error) error {
	switch status.Code(err) {
	case codes.NotFound:
		return &kv.KeyNotFoundError{KeyPath: key}
	case codes.FailedPrecondition:
		return fmt.Errorf("gcp: secret version disabled or destroyed for %q: %w", key, err)
	case codes.InvalidArgument:
		return fmt.Errorf("gcp: invalid secret request for %q: %w", key, err)
	default:
		return &kv.StoreUnavailableError{KeyPath: key, Err: err}
	}
}

func (gp *gcpProvider) Set(ctx context.Context, key, value string) error {
	return nil
}

func (gp *gcpProvider) Timeout() time.Duration {
	return gp.timeout
}

func (gp *gcpProvider) Close(_ context.Context) error {
	if gp.client != nil {
		err := gp.client.Close()
		if err != nil {
			return fmt.Errorf("gcp: close client: %w", err)
		}
	}

	return nil
}
