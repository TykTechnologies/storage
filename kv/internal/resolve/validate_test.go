package resolve

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestValidateSyntaxAll(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		doc     string
		wantErr bool
	}{
		{name: "no kv syntax at all", doc: `{"a":"plain","b":["x",1,true,null]}`},
		{name: "empty object", doc: `{}`},
		{
			name: "well-formed references at various depths",
			doc: `{"url":"https://$kv{env:HOST}/v1",` +
				`"secret":"kv://vault/db/creds#password",` +
				`"list":["kv://consul/services/web"]}`,
		},
		{
			name: "malformed whole-value reference in a nested non-URL field",
			doc: `{"middleware":{"operations":{"getget":{"transformRequestHeaders":` +
				`{"add":[{"name":"X-KV","value":"kv://random-gcpdb-password"}]}}}}}`,
			wantErr: true,
		},
		{
			name:    "unclosed inline token in an arbitrary string field",
			doc:     `{"headers":{"value":"prefix-$kv{unclosed"}}`,
			wantErr: true,
		},
		{
			name:    "inline token missing store separator in an array element",
			doc:     `{"values":["ok","$kv{missingcolon}"]}`,
			wantErr: true,
		},
		{
			name:    "empty store in a whole-value reference",
			doc:     `{"x":"kv:///secret/path"}`,
			wantErr: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			err := ValidateSyntaxAll([]byte(tc.doc))

			if tc.wantErr {
				require.ErrorIs(t, err, ErrMalformedReference)
				return
			}

			require.NoError(t, err)
		})
	}
}

func TestValidateSyntaxAll_InvalidJSON(t *testing.T) {
	t.Parallel()
	// A document that contains KV syntax but is not valid JSON cannot be walked.
	err := ValidateSyntaxAll([]byte(`{"x":"kv://vault/ok" `))
	require.ErrorIs(t, err, ErrInvalidJSON)
}

func TestValidateSyntaxAllAgreesWithResolveAllOnMalformed(t *testing.T) {
	t.Parallel()

	malformed := []string{
		`{"headers":[{"value":"kv://no-path-separator"}]}`,
		`{"nested":{"deep":{"v":"prefix-$kv{unclosed"}}}`,
		`{"arr":["ok","$kv{missingcolon}"]}`,
	}

	r := NewResolver(nil)

	for _, doc := range malformed {
		t.Run(doc, func(t *testing.T) {
			t.Parallel()
			require.ErrorIs(t, ValidateSyntaxAll([]byte(doc)), ErrMalformedReference)
			_, err := r.ResolveAll(context.Background(), []byte(doc))
			require.Error(t, err)
		})
	}
}

func TestValidateSyntaxAll_ErrorIncludesFieldPath(t *testing.T) {
	t.Parallel()

	doc := `{"x-tyk-api-gateway":{"middleware":{"operations":{"getget":` +
		`{"transformRequestHeaders":{"add":[{"name":"X-KV",` +
		`"value":"kv://random-gcpdb-password"}]}}}}}}`
	err := ValidateSyntaxAll([]byte(doc))

	require.ErrorIs(t, err, ErrMalformedReference)
	require.Contains(t, err.Error(),
		"x-tyk-api-gateway.middleware.operations.getget.transformRequestHeaders.add[0].value")
	require.Contains(t, err.Error(), "missing path separator")
	require.Contains(t, err.Error(), "kv://random-gcpdb-password")
	require.NotContains(t, err.Error(), `field "`)
}

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

			_, err := r.Resolve(t.Context(), input)
			require.ErrorIs(t, err, ErrMalformedReference,
				"Resolve must also error on an input ValidateSyntax rejects")
		})
	}
}
