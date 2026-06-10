package resolver

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestExtractJSONPointer(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		raw      string
		fragment string
		want     string
		wantErr  error
	}{
		// --- happy path: leaf types ---
		{
			name:     "top-level string field",
			raw:      `{"password":"hunter2"}`,
			fragment: "password",
			want:     "hunter2",
		},
		{
			name:     "top-level string field with leading slash",
			raw:      `{"password":"hunter2"}`,
			fragment: "/password",
			want:     "hunter2",
		},
		{
			name:     "top-level number field",
			raw:      `{"port":5432}`,
			fragment: "/port",
			want:     "5432",
		},
		{
			name:     "top-level bool field",
			raw:      `{"enabled":true}`,
			fragment: "/enabled",
			want:     "true",
		},
		{
			name:     "top-level null field",
			raw:      `{"value":null}`,
			fragment: "/value",
			want:     "null",
		},
		// --- happy path: traversal ---
		{
			name:     "nested object",
			raw:      `{"db":{"host":"localhost"}}`,
			fragment: "/db/host",
			want:     "localhost",
		},
		{
			name:     "array index zero",
			raw:      `{"hosts":["primary","replica"]}`,
			fragment: "/hosts/0",
			want:     "primary",
		},
		{
			name:     "array index non-zero",
			raw:      `{"hosts":["primary","replica"]}`,
			fragment: "/hosts/1",
			want:     "replica",
		},
		{
			name:     "object leaf re-serialized as JSON",
			raw:      `{"db":{"host":"localhost","port":5432}}`,
			fragment: "/db",
			want:     "", // non-empty JSON, checked separately below
		},
		{
			name:     "object then object then array index",
			raw:      `{"wow":{"some":["first","second"]}}`,
			fragment: "/wow/some/1",
			want:     "second",
		},
		// --- happy path: RFC 6901 escaping ---
		{
			name:     "tilde-one unescaped to slash in key",
			raw:      `{"a/b":"value"}`,
			fragment: "/a~1b",
			want:     "value",
		},
		{
			name:     "tilde-zero unescaped to tilde in key",
			raw:      `{"a~b":"value"}`,
			fragment: "/a~0b",
			want:     "value",
		},
		{
			name:     "combined tilde escapes",
			raw:      `{"a/b":{"c~d":"value"}}`,
			fragment: "/a~1b/c~0d",
			want:     "value",
		},
		// --- error: missing field ---
		{
			name:     "missing top-level key",
			raw:      `{"password":"secret"}`,
			fragment: "/username",
			wantErr:  ErrFieldNotFound,
		},
		{
			name:     "missing nested key",
			raw:      `{"db":{"host":"localhost"}}`,
			fragment: "/db/port",
			wantErr:  ErrFieldNotFound,
		},
		{
			name:     "array index out of bounds",
			raw:      `{"hosts":["primary"]}`,
			fragment: "/hosts/5",
			wantErr:  ErrFieldNotFound,
		},
		{
			name:     "array index negative",
			raw:      `{"hosts":["primary"]}`,
			fragment: "/hosts/-1",
			wantErr:  ErrFieldNotFound,
		},
		{
			name:     "traversing into a scalar",
			raw:      `{"port":5432}`,
			fragment: "/port/nested",
			wantErr:  ErrFieldNotFound,
		},
		{
			name:     "object then object then array index out of bounds",
			raw:      `{"wow":{"some":["first","second"]}}`,
			fragment: "/wow/some/5",
			wantErr:  ErrFieldNotFound,
		},
		// --- error: invalid JSON ---
		{
			name:     "payload is not JSON",
			raw:      "not-json",
			fragment: "/field",
			wantErr:  ErrInvalidJSON,
		},
		{
			name:     "empty payload",
			raw:      "",
			fragment: "/field",
			wantErr:  ErrInvalidJSON,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			got, err := extractJSONPointer(tc.raw, tc.fragment)

			if tc.wantErr != nil {
				require.Error(t, err)
				assert.True(t, errors.Is(err, tc.wantErr), "want %v, got %v", tc.wantErr, err)
				return
			}

			require.NoError(t, err)

			if tc.want == "" {
				assert.NotEmpty(t, got)
				return
			}

			assert.Equal(t, tc.want, got)
		})
	}
}
