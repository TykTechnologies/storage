package resolve_test

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/TykTechnologies/storage/kv"
	"github.com/TykTechnologies/storage/kv/internal/resolve"
	"github.com/stretchr/testify/require"
)

func BenchmarkResolve_ThreeInlineTokensWithFragment(b *testing.B) {
	payload := `{"host":"db.internal","port":"5432"}`
	getter := newGetter(map[string]kv.Provider{
		"vault": &mockProvider{value: payload},
		"env":   &mockProvider{value: "simple-value"},
	})
	r := resolve.NewResolver(getter)
	input := "postgres://$kv{vault:db/creds#host}:$kv{vault:db/creds#port}/$kv{env:DB_NAME}"
	ctx := context.Background()

	b.ResetTimer()

	for b.Loop() {
		_, err := r.Resolve(ctx, input)
		if err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkResolveAll measures the whole-document resolution cost the caller
// pays at startup as a function of reference count. Providers are
// in-memory mocks, so the numbers isolate the library's own overhead
// — parse, walk, substitute, re-serialize — from backend latency.
func BenchmarkResolveAll(b *testing.B) {
	getter := newGetter(map[string]kv.Provider{
		"env":   &mockProvider{value: "resolved-value"},
		"vault": &mockProvider{value: `{"username":"admin","password":"hunter2"}`},
	})
	r := resolve.NewResolver(getter)
	ctx := context.Background()

	for _, n := range []int{20, 50, 100} {
		doc := buildBenchDoc(b, n)

		b.Run(fmt.Sprintf("refs=%d", n), func(b *testing.B) {
			for b.Loop() {
				if _, err := r.ResolveAll(ctx, doc); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

const benchLatency = time.Millisecond

func BenchmarkResolveAll_DuplicateRefs(b *testing.B) {
	for _, count := range []int{1, 10, 100, 1000, 10000} {
		b.Run(fmt.Sprintf("count=%d", count), func(b *testing.B) {
			provider := &latencyProvider{value: "s3cr3t", latency: benchLatency}
			r := resolve.NewResolver(newGetter(map[string]kv.Provider{"vault": provider}))
			doc := buildDoc("kv://vault/secret/data/app", count)

			ctx := context.Background()

			b.ReportAllocs()
			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				if _, err := r.ResolveAll(ctx, doc); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkResolveAll_DistinctRefs(b *testing.B) {
	for _, count := range []int{1, 16, 64, 256} {
		b.Run(fmt.Sprintf("count=%d", count), func(b *testing.B) {
			provider := &latencyProvider{value: "v", latency: benchLatency}
			r := resolve.NewResolver(newGetter(map[string]kv.Provider{"vault": provider}))
			doc := distinctRefsDoc(count)

			ctx := context.Background()

			b.ReportAllocs()
			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				if _, err := r.ResolveAll(ctx, doc); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkResolveAll_NoRefs(b *testing.B) {
	provider := &latencyProvider{value: "unused", latency: benchLatency}
	r := resolve.NewResolver(newGetter(map[string]kv.Provider{"vault": provider}))

	fields := make([]string, 1000)
	for i := range fields {
		fields[i] = fmt.Sprintf(`"api_%d":{"listen_path":"/svc-%d/","target":"http://upstream-%d.internal"}`, i, i, i)
	}

	doc := []byte("{" + strings.Join(fields, ",") + "}")
	ctx := context.Background()

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		if _, err := r.ResolveAll(ctx, doc); err != nil {
			b.Fatal(err)
		}
	}
}

type latencyProvider struct {
	value   string
	latency time.Duration
}

func (p *latencyProvider) Get(ctx context.Context, _ string) (string, error) {
	select {
	case <-time.After(p.latency):
		return p.value, nil
	case <-ctx.Done():
		return "", ctx.Err()
	}
}

func buildBenchDoc(b *testing.B, n int) []byte {
	b.Helper()

	fields := make(map[string]any, n*2)

	for i := 0; i < n; i++ {
		key := fmt.Sprintf("field_%d", i)

		switch i % 3 {
		case 0:
			fields[key] = "kv://env/SOME_KEY"
		case 1:
			fields[key] = fmt.Sprintf("https://$kv{env:HOST_%d}/v1", i)
		case 2:
			fields[key] = "kv://vault/db/creds#password"
		}

		fields[fmt.Sprintf("plain_%d", i)] = "no reference here"
	}

	doc, err := json.Marshal(map[string]any{"config": fields})
	require.NoError(b, err)

	return doc
}

func distinctRefsDoc(count int) []byte {
	fields := make([]string, count)
	for i := range fields {
		fields[i] = fmt.Sprintf(`"h%d":"kv://vault/secret/%d"`, i, i)
	}

	return []byte("{" + strings.Join(fields, ",") + "}")
}
