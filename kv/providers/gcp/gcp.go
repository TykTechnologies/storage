package gcp

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"net/url"
	"os"
	"slices"
	"strings"
	"time"

	"cloud.google.com/go/auth/credentials"
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

// Validation rules were grabbed from:
// https://docs.cloud.google.com/docs/authentication/client-libraries#validate_other_credential_configurations
func validateExternalAccount(config *Config) error {
	raw := []byte(config.CredentialsJSON)

	if config.CredentialsFile != "" {
		b, err := os.ReadFile(config.CredentialsFile)
		if err != nil {
			return fmt.Errorf("gcp: read external_account credentials_file: %w", err)
		}

		raw = b
	}

	var ea struct {
		Type                           string `json:"type"`
		TokenURL                       string `json:"token_url"`
		ServiceAccountImpersonationURL string `json:"service_account_impersonation_url"`
		CredentialSource               struct {
			Executable json.RawMessage `json:"executable"`
		} `json:"credential_source"`
	}

	if err := json.Unmarshal(raw, &ea); err != nil {
		return fmt.Errorf("gcp: invalid external_account config: %w", err)
	}

	if ea.Type != "external_account" {
		return fmt.Errorf("gcp: external_account config type is %q, want external_account", ea.Type)
	}

	if ea.TokenURL != "" && !isGoogleHost(ea.TokenURL) {
		return fmt.Errorf("gcp: external_account token_url %q is not a Google endpoint", ea.TokenURL)
	}

	if ea.ServiceAccountImpersonationURL != "" && !isGoogleHost(ea.ServiceAccountImpersonationURL) {
		return fmt.Errorf(
			"gcp: external_account service_account_impersonation_url %q is not a Google endpoint",
			ea.ServiceAccountImpersonationURL,
		)
	}

	if len(ea.CredentialSource.Executable) > 0 {
		return errors.New("gcp: external_account executable credential source is not permitted")
	}

	return nil
}

func isGoogleHost(rawURL string) bool {
	u, err := url.Parse(rawURL)
	if err != nil {
		return false
	}

	host := u.Hostname()

	return host == "googleapis.com" || strings.HasSuffix(host, ".googleapis.com")
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

//   - [ ] **T8 — Auth paths.** Wire `impersonate.NewCredentials` (ADC + explicit `DetectDefault`
//     base), WIF via `external_account` + `validateExternalAccount` (§15.8), and
//     `quota_project_id` (`WithQuotaProject`); confirm the nil-base note (§8.2).
//     _Verify:_ factory + external-account-validation + option-assembly tests + real-E2E smoke.
//
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
		_, err := credentials.DetectDefault(&credentials.DetectOptions{})
		if err != nil {
			return fmt.Errorf("gcp: detect ADC: %w", err)
		}

		credType := option.CredentialsType(gp.cfg.CredentialsType)

		if gp.cfg.CredentialsFile != "" {
			opts = append(opts, option.WithAuthCredentialsFile(credType, gp.cfg.CredentialsFile))
		}

		if gp.cfg.CredentialsJSON != "" {
			opts = append(opts, option.WithAuthCredentialsJSON(credType, []byte(gp.cfg.CredentialsJSON)))
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

func (gp *gcpProvider) Set(ctx context.Context, key, value string) error {
	err := gp.validateSecretKey(key)
	if err != nil {
		return err
	}

	if strings.Contains(key, versionsPath) {
		return fmt.Errorf("gcp: set does not accept version in the key %q", key)
	}

	ctx, cancel := context.WithTimeout(ctx, gp.getTimeout())
	defer cancel()

	data := []byte(value)
	crc := int64(crc32.Checksum(data, castagnoli))
	payload := &secretmanagerpb.SecretPayload{
		Data:       data,
		DataCrc32C: &crc,
	}

	secretName := gp.secretName(key)

	addReq := &secretmanagerpb.AddSecretVersionRequest{
		Parent:  secretName,
		Payload: payload,
	}

	_, err = gp.client.AddSecretVersion(ctx, addReq)
	if err == nil {
		return nil
	}

	// Not found is not an error in this case, it means the process should
	// create the secret first.
	if status.Code(err) != codes.NotFound {
		return gp.classify(key, err)
	}

	secret := &secretmanagerpb.Secret{}

	if gp.cfg.Location == "" {
		// Adding replication policy to replicate secret payload across
		// multiple regions globally.
		secret.Replication = &secretmanagerpb.Replication{
			Replication: &secretmanagerpb.Replication_Automatic_{
				Automatic: &secretmanagerpb.Replication_Automatic{},
			},
		}
	}

	createReq := &secretmanagerpb.CreateSecretRequest{
		Parent:   gp.projectParent(),
		SecretId: key,
		Secret:   secret,
	}

	if _, err = gp.client.CreateSecret(ctx, createReq); err != nil && status.Code(err) != codes.AlreadyExists {
		return gp.classify(key, err)
	}

	if _, err = gp.client.AddSecretVersion(ctx, addReq); err != nil {
		return gp.classify(key, err)
	}

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

func (gp *gcpProvider) getTimeout() time.Duration {
	if gp.timeout > 0 {
		return gp.timeout
	}

	// TODO: Do I have to create some const and share it across internal/store and gcp provider?
	return 5 * time.Second
}
