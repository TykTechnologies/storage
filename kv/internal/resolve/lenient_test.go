package resolve_test

import (
	"context"
	"testing"

	"github.com/TykTechnologies/storage/kv"
	"github.com/TykTechnologies/storage/kv/internal/resolve"
	"github.com/stretchr/testify/require"
)

func TestLenientResolve(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		stores  map[string]kv.Provider
		input   string
		want    string
		wantErr error
	}{
		{
			name:  "whole-value reference to absent store returned unchanged",
			input: "kv://vault/db/creds#password",
			want:  "kv://vault/db/creds#password",
		},
		{
			name:   "inline token to absent store left in place",
			stores: map[string]kv.Provider{"env": &mockProvider{value: "myhost.com"}},
			input:  "https://$kv{env:API_HOST}/$kv{vault:db/creds#password}",
			want:   "https://myhost.com/$kv{vault:db/creds#password}",
		},
		{
			name:  "multiple inline tokens to absent stores all left in place",
			input: "$kv{vault:a}/$kv{consul:b}",
			want:  "$kv{vault:a}/$kv{consul:b}",
		},
		{
			name:   "fully resolvable input resolves exactly as in strict mode",
			stores: map[string]kv.Provider{"env": &mockProvider{value: "s3cr3t"}},
			input:  "kv://env/TOKEN",
			want:   "s3cr3t",
		},
		{
			name:  "plain string without references passes through",
			input: "no references here",
			want:  "no references here",
		},
		{
			name: "present store with missing key still errors",
			stores: map[string]kv.Provider{
				"env": &mockProvider{err: &kv.KeyNotFoundError{StoreName: "env", KeyPath: "MISSING"}},
			},
			input:   "kv://env/MISSING",
			wantErr: &kv.KeyNotFoundError{},
		},
		{
			name:    "malformed whole-value reference still errors",
			input:   "kv://no-path-separator",
			wantErr: resolve.ErrMalformedReference,
		},
		{
			name:    "malformed inline token still errors",
			input:   "prefix-$kv{missing-store-separator}-suffix",
			wantErr: resolve.ErrMalformedReference,
		},
		{
			name:   "missing JSON field on a reachable store still errors",
			stores: map[string]kv.Provider{"env": &mockProvider{value: `{"user":"admin"}`}},
			input:  "kv://env/creds#missing_field",

			wantErr: resolve.ErrFieldNotFound,
		},
		{
			name: "absent-store token coexists with an error on a reachable store",
			stores: map[string]kv.Provider{
				"env": &mockProvider{err: &kv.KeyNotFoundError{StoreName: "env", KeyPath: "A"}},
			},
			input:   "$kv{env:A}/$kv{vault:b}",
			wantErr: &kv.KeyNotFoundError{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			r := resolve.NewResolver(newGetter(tt.stores), resolve.WithLenientMode())

			got, err := r.Resolve(t.Context(), tt.input)

			if tt.wantErr != nil {
				switch want := tt.wantErr.(type) {
				case *kv.KeyNotFoundError:
					var keyErr *kv.KeyNotFoundError
					require.ErrorAs(t, err, &keyErr)
				default:
					require.ErrorIs(t, err, want)
				}

				return
			}

			require.NoError(t, err)
			require.Equal(t, tt.want, got)
		})
	}
}

func TestLenientResolveAll(t *testing.T) {
	t.Parallel()

	t.Run("absent-store references pass through, resolvable ones resolve", func(t *testing.T) {
		t.Parallel()

		r := resolve.NewResolver(
			newGetter(map[string]kv.Provider{"env": &mockProvider{value: "hvs.secret-token"}}),
			resolve.WithLenientMode(),
		)

		doc := []byte(`{
			"token": "kv://env/VAULT_TOKEN",
			"credentials": "kv://vault/aws-creds",
			"url": "https://$kv{env:VAULT_TOKEN}@$kv{consul:host}"
		}`)

		got, err := r.ResolveAll(context.Background(), doc)
		require.NoError(t, err)
		require.JSONEq(t, `{
			"token": "hvs.secret-token",
			"credentials": "kv://vault/aws-creds",
			"url": "https://hvs.secret-token@$kv{consul:host}"
		}`, string(got))
	})

	t.Run("malformed reference inside the document still errors", func(t *testing.T) {
		t.Parallel()

		r := resolve.NewResolver(newGetter(nil), resolve.WithLenientMode())

		_, err := r.ResolveAll(context.Background(), []byte(`{"bad": "kv://no-path-separator"}`))
		require.ErrorIs(t, err, resolve.ErrMalformedReference)
	})

	t.Run("invalid JSON still errors", func(t *testing.T) {
		t.Parallel()

		r := resolve.NewResolver(newGetter(nil), resolve.WithLenientMode())

		_, err := r.ResolveAll(context.Background(), []byte(`{not json`))
		require.ErrorIs(t, err, resolve.ErrInvalidJSON)
	})
}

// Strict mode is the default and must be completely unaffected by the
// existence of the lenient option.
func TestStrictModeRemainsDefault(t *testing.T) {
	t.Parallel()

	t.Run("whole-value reference to absent store errors", func(t *testing.T) {
		t.Parallel()

		r := resolve.NewResolver(newGetter(nil))

		_, err := r.Resolve(context.Background(), "kv://vault/db/creds")
		require.ErrorIs(t, err, kv.ErrStoreNotFound)
	})

	t.Run("inline token to absent store errors", func(t *testing.T) {
		t.Parallel()

		r := resolve.NewResolver(newGetter(nil))

		_, err := r.Resolve(context.Background(), "https://$kv{vault:host}/v1")
		require.ErrorIs(t, err, kv.ErrStoreNotFound)
	})
}
