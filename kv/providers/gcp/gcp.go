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

	"cloud.google.com/go/auth"
	"cloud.google.com/go/auth/credentials"
	"cloud.google.com/go/auth/credentials/impersonate"
	secretmanager "cloud.google.com/go/secretmanager/apiv1"
	secretmanagerpb "cloud.google.com/go/secretmanager/apiv1/secretmanagerpb"
	"github.com/TykTechnologies/storage/kv"
	"google.golang.org/api/option"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	versionsPath       = "/versions/"
	cloudPlatformScope = "https://www.googleapis.com/auth/cloud-platform"
)

var (
	allowedCredentialsTypes = []string{"service_account", "authorized_user", "external_account"}
	allowedTransports       = []string{"grpc", "rest"}
	castagnoli              = crc32.MakeTable(crc32.Castagnoli)

	_ kv.Setter      = (*gcpProvider)(nil)
	_ kv.Initializer = (*gcpProvider)(nil)
	_ kv.Timeouter   = (*gcpProvider)(nil)
	_ kv.Closer      = (*gcpProvider)(nil)
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

// NewFactory returns the factory that validates a store's config and builds the
// provider. Validation is exhaustive and fails loud here;
// the client is built later in Init, and no network call is made.
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

// gcpProvider is one configured store: a single Secret Manager client (built in
// Init, reused across concurrent calls) plus its resolved config and timeout.
// testOpts, when set, replaces the assembled client options in tests.
type gcpProvider struct {
	cfg      *Config
	client   *secretmanager.Client
	timeout  time.Duration
	testOpts []option.ClientOption
}

// Init builds the one client. The client dials lazily and Init runs no
// reachability check, so a transient outage at boot doesn't disable the store.
func (gp *gcpProvider) Init(ctx context.Context) error {
	opts := gp.testOpts
	if len(opts) == 0 {
		built, err := gp.buildAuthOptions(ctx)
		if err != nil {
			return err
		}

		opts = built
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

// Get fetches a secret version's payload and returns it verbatim. It verifies the
// server CRC32C when present, then optionally trims one trailing newline; RPC
// errors are mapped to typed kv errors.
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

// Set adds a new secret version, creating the secret container first if it does
// not exist (the normal first write). It self-bounds with a timeout because it is
// reached outside the SecretStore wrapper, and rejects version-pinned keys since
// versions are server-assigned.
func (gp *gcpProvider) Set(ctx context.Context, key, value string) error {
	err := gp.validateSecretKey(key)
	if err != nil {
		return err
	}

	if strings.Contains(key, versionsPath) {
		return fmt.Errorf("gcp: set does not accept version in the key %q", key)
	}

	ctx, cancel := context.WithTimeout(ctx, gp.setTimeout())
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

// Timeout is the per-call bound the SecretStore wrapper applies to Get; 0 means
// the wrapper's default.
func (gp *gcpProvider) Timeout() time.Duration {
	return gp.timeout
}

// Close closes the client. It is nil-safe so the registry can call it after a
// partial init.
func (gp *gcpProvider) Close(_ context.Context) error {
	if gp.client != nil {
		err := gp.client.Close()
		if err != nil {
			return fmt.Errorf("gcp: close client: %w", err)
		}
	}

	return nil
}

// buildAuthOptions assembles the client options for the real (non-test) path:
// regional endpoint, quota project, and exactly one credential source —
// impersonation, an explicit key, or (the default) ADC resolved lazily.
func (gp *gcpProvider) buildAuthOptions(ctx context.Context) ([]option.ClientOption, error) {
	var opts []option.ClientOption

	if ep := gp.endpoint(); ep != "" {
		opts = append(opts, option.WithEndpoint(ep))
	}

	if gp.cfg.QuotaProjectID != "" {
		opts = append(opts, option.WithQuotaProject(gp.cfg.QuotaProjectID))
	}

	hasFile := gp.cfg.CredentialsFile != ""
	hasJSON := gp.cfg.CredentialsJSON != ""

	switch {
	case gp.cfg.ImpersonateServiceAccount != "":
		var base *auth.Credentials

		// At first we want to get Base creds for impersonation
		if hasFile || hasJSON {
			var err error

			base, err = credentials.DetectDefault(&credentials.DetectOptions{
				CredentialsFile: gp.cfg.CredentialsFile,
				CredentialsJSON: []byte(gp.cfg.CredentialsJSON),
				Scopes:          []string{cloudPlatformScope},
			})
			if err != nil {
				return nil, fmt.Errorf("gcp: build impersonation base credentials: %w", err)
			}
		}

		creds, err := impersonate.NewCredentials(&impersonate.CredentialsOptions{
			TargetPrincipal: gp.cfg.ImpersonateServiceAccount,
			Delegates:       gp.cfg.ImpersonateDelegates,
			Scopes:          []string{cloudPlatformScope},
			Credentials:     base,
		})
		if err != nil {
			return nil, fmt.Errorf("gcp: build impersonated credentials: %w", err)
		}

		opts = append(opts, option.WithAuthCredentials(creds))

	case hasFile:
		opts = append(opts, option.WithAuthCredentialsFile(
			option.CredentialsType(gp.cfg.CredentialsType),
			gp.cfg.CredentialsFile,
		))
	case hasJSON:
		opts = append(opts, option.WithAuthCredentialsJSON(
			option.CredentialsType(gp.cfg.CredentialsType),
			[]byte(gp.cfg.CredentialsJSON),
		))
	}
	// default: ADC is resolved lazily by the client

	return opts, nil
}

// endpoint returns the regional endpoint override, or "" for a global store
// (which uses the SDK default). Only the format differs by transport: gRPC dials
// host:443, REST needs an https URL.
func (gp *gcpProvider) endpoint() string {
	if gp.cfg.Location == "" {
		return ""
	}

	host := "secretmanager." + gp.cfg.Location + ".rep.googleapis.com"
	if gp.cfg.Transport == "rest" {
		return "https://" + host
	}

	return host + ":443"
}

// validateSecretKey rejects an empty key and any full or multi-segment resource
// name. A key is always a bare secret ID scoped to the configured project.
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

// versionName builds the AccessSecretVersion resource name, defaulting to the
// latest version when the key pins none.
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

// classify maps a gRPC status to a kv error in three buckets: NotFound is a
// missing key; FailedPrecondition (disabled/destroyed version) and InvalidArgument
// (malformed request) are permanent caller errors returned plainly, so the cache
// can't hide the real cause behind a retry; everything else is a transient outage.
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

// setTimeout is the per-call deadline for Set, which runs outside the
// SecretStore wrapper that bounds Get and so must bound itself.
func (gp *gcpProvider) setTimeout() time.Duration {
	// Fallback mirrors the SecretStore default, so an unconfigured Set behaves like Get.
	const defaultTimeout = 5 * time.Second

	if gp.timeout > 0 {
		return gp.timeout
	}

	return defaultTimeout
}
