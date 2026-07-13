package resolve

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestParseWholeValue(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		input   string
		wantOK  bool
		wantErr bool
		want    refKey
	}{
		{
			name:   "path only",
			input:  "kv://vault/secret/data/app",
			wantOK: true,
			want:   refKey{store: "vault", path: "secret/data/app"},
		},
		{
			name:   "with fragment",
			input:  "kv://vault/secret/data/app#password",
			wantOK: true,
			want:   refKey{store: "vault", path: "secret/data/app", fragment: "password"},
		},
		{
			name:   "fragment splits on first hash only",
			input:  "kv://vault/a/b#x/y#z",
			wantOK: true,
			want:   refKey{store: "vault", path: "a/b", fragment: "x/y#z"},
		},
		{
			name:   "not a whole-value reference",
			input:  "prefix-$kv{vault:secret}-suffix",
			wantOK: false,
		},
		{
			name:   "plain literal",
			input:  "just-a-string",
			wantOK: false,
		},
		{
			name:    "missing path separator",
			input:   "kv://vault",
			wantOK:  true,
			wantErr: true,
		},
		{
			name:    "empty store",
			input:   "kv:///secret/data/app",
			wantOK:  true,
			wantErr: true,
		},
		{
			name:    "empty path",
			input:   "kv://vault/",
			wantOK:  true,
			wantErr: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			ref, ok, err := parseWholeValue(tc.input)

			require.Equal(t, tc.wantOK, ok)

			if tc.wantErr {
				require.ErrorIs(t, err, ErrMalformedReference)
				return
			}

			require.NoError(t, err)
			require.Equal(t, tc.want, ref)
		})
	}
}

func TestParseInlineToken(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		match   string
		wantErr bool
		want    refKey
	}{
		{
			name:  "path only",
			match: "$kv{vault:secret/data/app}",
			want:  refKey{store: "vault", path: "secret/data/app"},
		},
		{
			name:  "with fragment",
			match: "$kv{vault:secret/data/app#password}",
			want:  refKey{store: "vault", path: "secret/data/app", fragment: "password"},
		},
		{
			name:    "missing store separator",
			match:   "$kv{no-colon-here}",
			wantErr: true,
		},
		{
			name:    "empty store",
			match:   "$kv{:secret}",
			wantErr: true,
		},
		{
			name:    "empty path",
			match:   "$kv{vault:}",
			wantErr: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			ref, err := parseInlineToken(tc.match)

			if tc.wantErr {
				require.ErrorIs(t, err, ErrMalformedReference)
				return
			}

			require.NoError(t, err)
			require.Equal(t, tc.want, ref)
		})
	}
}

func TestCollectRefsMatchesResolverParsing(t *testing.T) {
	t.Parallel()

	doc := map[string]any{
		"whole":     "kv://vault/secret/data/app#password",
		"inline":    "postgres://$kv{vault:db/creds#host}:$kv{env:DB_PORT}/prod",
		"dupe":      "kv://vault/secret/data/app#password", // same target as "whole"
		"malformed": "kv://vault",                          // no path — skipped
		"literal":   "no references here",
		"nested": map[string]any{
			"list": []any{"kv://consul/services/web", "plain"},
		},
	}

	got := collectRefs(doc)

	want := map[refKey]struct{}{
		{store: "vault", path: "secret/data/app", fragment: "password"}: {},
		{store: "vault", path: "db/creds", fragment: "host"}:            {},
		{store: "env", path: "DB_PORT"}:                                 {},
		{store: "consul", path: "services/web"}:                         {},
	}

	require.Equal(t, want, got)
}
