package gcp

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"os"
	"strings"
)

// validateExternalAccount checks a WIF (external_account) config before the SDK
// uses it — the SDK deliberately does not. A tampered token_url or AWS metadata
// URL is an exfiltration/SSRF vector and an executable source is RCE, so each is
// rejected against Google's expected-value table.
// Source: https://docs.cloud.google.com/docs/authentication/client-libraries#validate_other_credential_configurations
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
			URL                   string          `json:"url"`
			Executable            json.RawMessage `json:"executable"`
			EnvironmentID         string          `json:"environment_id"`
			RegionURL             string          `json:"region_url"`
			IMDSv2SessionTokenURL string          `json:"imdsv2_session_token_url"`
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

	cs := ea.CredentialSource
	if strings.HasPrefix(cs.EnvironmentID, "aws") {
		for _, u := range []string{cs.URL, cs.RegionURL, cs.IMDSv2SessionTokenURL} {
			if u != "" && !isAWSIMDSHost(u) {
				return fmt.Errorf("gcp: external_account aws credential source url %q is not the AWS IMDS endpoint", u)
			}
		}
	}

	return nil
}
func isGoogleHost(rawURL string) bool {
	u, err := url.Parse(rawURL)
	if err != nil {
		return false
	}

	host := u.Hostname()

	// Source: https://docs.cloud.google.com/docs/authentication/client-libraries#validate_other_credential_configurations
	return host == "googleapis.com" || strings.HasSuffix(host, ".googleapis.com")
}

func isAWSIMDSHost(rawURL string) bool {
	u, err := url.Parse(rawURL)
	if err != nil {
		return false
	}

	host := u.Hostname()

	// Source: https://docs.cloud.google.com/docs/authentication/client-libraries#validate_other_credential_configurations
	return host == "169.254.169.254" || host == "fd00:ec2::254"
}
