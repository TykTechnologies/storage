package resolve

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestValidateSyntax(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		input   string
		wantErr bool
	}{
		// --- no KV syntax: always valid ---
		{name: "empty string", input: ""},
		{name: "plain literal", input: "just-a-string"},
		{name: "ordinary url without a kv reference", input: "https://example.com/api"},

		// --- well-formed whole-value references ---
		{name: "whole-value path only", input: "kv://vault/secret/data/app"},
		{name: "whole-value with fragment", input: "kv://vault/secret/data/app#password"},
		{name: "whole-value env", input: "kv://env/API_HOST"},

		// --- well-formed inline tokens ---
		{name: "inline in url path", input: "https://$kv{env:API_HOST}/v1"},
		{name: "inline with fragment", input: "https://$kv{vault:db/creds#host}/v1"},
		{name: "inline as whole value", input: "$kv{vault:secret/data/app}"},
		{name: "multiple inline tokens", input: "postgres://$kv{vault:db/creds#host}:$kv{env:DB_PORT}/prod"},

		// --- whole-value precedence: $kv{ inside a kv:// path is literal, not inline ---
		{name: "kv:// path containing an unclosed $kv{ is literal", input: "kv://vault/path$kv{unclosed"},

		// --- malformed inline tokens ---
		{name: "inline unclosed token", input: "https://example.com/$kv{unclosed", wantErr: true},
		{name: "inline unclosed after a valid token", input: "$kv{env:HOST}/$kv{unclosed", wantErr: true},
		{name: "inline missing store separator", input: "https://example.com/$kv{missingcolon}", wantErr: true},
		{name: "inline empty store", input: "https://$kv{:onlypath}/x", wantErr: true},
		{name: "inline empty path", input: "https://$kv{store:}/x", wantErr: true},

		// --- malformed whole-value references ---
		{name: "whole-value missing path separator", input: "kv://no-path-separator", wantErr: true},
		{name: "whole-value empty store", input: "kv:///secret/x", wantErr: true},
		{name: "whole-value empty path", input: "kv://vault/", wantErr: true},
		{name: "whole-value empty everything", input: "kv://", wantErr: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			err := ValidateSyntax(tc.input)

			if tc.wantErr {
				require.ErrorIs(t, err, ErrMalformedReference)
				return
			}

			require.NoError(t, err)
		})
	}
}

func TestValidateSyntaxAgreesWithResolveOnMalformed(t *testing.T) {
	t.Parallel()

	malformed := []string{
		"https://example.com/$kv{unclosed",
		"$kv{env:HOST}/$kv{unclosed",
		"https://example.com/$kv{missingcolon}",
		"https://$kv{:onlypath}/x",
		"https://$kv{store:}/x",
		"kv://no-path-separator",
		"kv:///secret/x",
		"kv://vault/",
		"kv://",
	}

	r := NewResolver(nil)

	for _, input := range malformed {
		t.Run(input, func(t *testing.T) {
			t.Parallel()

			require.ErrorIs(t, ValidateSyntax(input), ErrMalformedReference,
				"ValidateSyntax must reject this input")

			_, err := r.Resolve(context.Background(), input)
			require.Error(t, err,
				"Resolve must also error on an input ValidateSyntax rejects")
		})
	}
}
