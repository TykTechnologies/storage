package kv

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

type mockProvider struct{}

func (m *mockProvider) Get(ctx context.Context, path string) (string, error) {
	return "", nil
}

func (m *mockProvider) Unwrap() Provider {
	return m
}

func TestAs_CircularDependencyWontFail(t *testing.T) {
	t.Parallel()

	m := &mockProvider{}
	_, ok := As[Closer](m)

	require.False(t, ok)
}
