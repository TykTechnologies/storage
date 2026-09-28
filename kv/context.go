package kv

import "context"

type cacheBypassKey struct{}

// WithCacheBypass returns a context that instructs SecretStore.Get to skip
// the cache read, fetch from the backend, and re-populate the cache entry.
// It is context-carried because the call path (Resolver.Resolve →
// Registry.GetStore → Provider.Get) has fixed signatures with no room for
// an options parameter.
func WithCacheBypass(ctx context.Context) context.Context {
	return context.WithValue(ctx, cacheBypassKey{}, true)
}

// IsCacheBypassed reports whether context carries the cache-bypass directive.
func IsCacheBypassed(ctx context.Context) bool {
	bypassed, ok := ctx.Value(cacheBypassKey{}).(bool)
	if !ok {
		return false
	}

	return bypassed
}
