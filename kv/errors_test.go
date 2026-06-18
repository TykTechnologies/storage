package kv_test

import (
	"errors"
	"testing"

	"github.com/TykTechnologies/storage/kv"
	"github.com/stretchr/testify/require"
)

func TestKeyNotFoundError_Error(t *testing.T) {
	t.Parallel()

	t.Run("omits the store clause when StoreName is empty", func(t *testing.T) {
		t.Parallel()

		// Standalone providers (e.g. file) cannot know their registry name,
		// so they leave StoreName empty; the message must still read cleanly.
		err := &kv.KeyNotFoundError{KeyPath: "db/password"}
		require.Equal(t, `key "db/password" not found`, err.Error())
	})

	t.Run("includes the store name when present", func(t *testing.T) {
		t.Parallel()

		err := &kv.KeyNotFoundError{StoreName: "vault", KeyPath: "db/password"}
		require.Equal(t, `key "db/password" not found in store "vault"`, err.Error())
	})
}

func TestKeyNotFoundError_IsMatchable(t *testing.T) {
	t.Parallel()

	wrapped := errors.Join(errors.New("context"), &kv.KeyNotFoundError{KeyPath: "k"})

	var notFound *kv.KeyNotFoundError
	require.ErrorAs(t, wrapped, &notFound)
	require.Equal(t, "k", notFound.KeyPath)
}
