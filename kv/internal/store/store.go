package store

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/TykTechnologies/storage/kv"
	"golang.org/x/sync/singleflight"
)

// SecretStore is an internal decorator that adds singleflight deduplication,
// a bounded per-operation timeout, and closed-state gating to a Provider.
type SecretStore struct {
	name     string
	provider kv.Provider
	sf       *singleflight.Group
	isClosed atomic.Bool
	timeout  time.Duration
}

// Option defines a functional option for configuring the SecretStore.
type Option func(*SecretStore)

// Get retrieves a secret value with deduplication.
func (s *SecretStore) Get(ctx context.Context, path string) (string, error) {
	if s.isClosed.Load() {
		return "", kv.ErrStoreClosed
	}

	ch := s.sf.DoChan(path, func() (any, error) {
		// WithoutCancel: this fetch is shared by every caller waiting on the same
		// path, so one caller going away must not abort it for the others.
		fetchCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), s.timeout)
		defer cancel()

		val, err := s.provider.Get(fetchCtx, path)

		// Name the path in the error: a bare context deadline gives
		// no way to tell which secret timed out.
		if errors.Is(err, context.DeadlineExceeded) {
			return "", fmt.Errorf("timeout fetching %q: %w", path, err)
		}

		return val, err
	})

	select {
	case <-ctx.Done():
		return "", ctx.Err()
	case res := <-ch:
		if res.Err != nil {
			return "", res.Err
		}

		v, ok := res.Val.(string)
		if !ok {
			return "", fmt.Errorf(
				"%w: path %q returned non-string type",
				kv.ErrContractViolation,
				path,
			)
		}

		return v, nil
	}
}

// Unwrap allows callers to access the underlying provider for optional interfaces (like Lister)
func (s *SecretStore) Unwrap() kv.Provider {
	return s.provider
}

func (s *SecretStore) Close(ctx context.Context) error {
	if s.isClosed.Swap(true) {
		return nil
	}

	if closer, ok := kv.AsCloser(s.provider); ok {
		return closer.Close(ctx)
	}

	return nil
}

// WithTimeout overrides the global default provider timeout.
func WithTimeout(timeout time.Duration) Option {
	return func(store *SecretStore) {
		if timeout > 0 {
			store.timeout = timeout
		}
	}
}

// NewSecretStore instantiates the store wrapper with optional configurations.
func NewSecretStore(
	name string,
	provider kv.Provider,
	opts ...Option,
) (*SecretStore, error) {
	if provider == nil {
		return nil, fmt.Errorf("secret store %q: provider cannot be nil", name)
	}

	s := &SecretStore{
		name:     name,
		provider: provider,
		sf:       &singleflight.Group{},
		timeout:  kv.DefaultOperationTimeout,
	}

	for _, opt := range opts {
		opt(s)
	}

	return s, nil
}
