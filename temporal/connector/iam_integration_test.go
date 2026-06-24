package connector_test

import (
	"context"
	"os"
	"sync/atomic"
	"testing"

	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/TykTechnologies/storage/temporal/connector"
	"github.com/TykTechnologies/storage/temporal/model"
)

// TestIAMCredentialsProvider_Integration proves, against a real Redis/Valkey
// instance requiring auth, that:
//  1. a WithCredentialsProvider-supplied password authenticates a connection,
//  2. a wrong static password is genuinely rejected (auth is enforced), and
//  3. rotating the provider's returned value re-authenticates new connections
//     without rebuilding the connector.
//
// This mirrors GCP Memorystore IAM auth, where the provider returns a fresh
// short-lived token per new connection. The only piece it cannot exercise
// locally is Google's server-side verification of a Google-minted token.
//
// Run with:
//
//	TEST_IAM_REDIS_ADDR=localhost:6390 TEST_IAM_REDIS_PASS=tokenAAA \
//	  go test ./temporal/connector/ -run TestIAMCredentialsProvider_Integration -v
func TestIAMCredentialsProvider_Integration(t *testing.T) {
	addr := os.Getenv("TEST_IAM_REDIS_ADDR")
	initialPass := os.Getenv("TEST_IAM_REDIS_PASS")
	if addr == "" || initialPass == "" {
		t.Skip("set TEST_IAM_REDIS_ADDR and TEST_IAM_REDIS_PASS to run the IAM integration test")
	}

	ctx := context.Background()

	// token holds the value the provider will hand out; we rotate it mid-test.
	var token atomic.Value
	token.Store(initialPass)

	providerCalls := int32(0)
	provider := func(_ context.Context) (string, string, error) {
		atomic.AddInt32(&providerCalls, 1)
		return "default", token.Load().(string), nil
	}

	// 1. Correct credentials via the provider authenticate successfully.
	conn, err := connector.NewConnector(model.RedisV9Type,
		model.WithRedisConfig(&model.RedisOptions{Addrs: []string{addr}}),
		model.WithCredentialsProvider(provider),
	)
	require.NoError(t, err)
	require.NoError(t, conn.Ping(ctx), "provider-supplied password should authenticate")
	assert.Greater(t, atomic.LoadInt32(&providerCalls), int32(0), "provider must be consulted")
	require.NoError(t, conn.Disconnect(ctx))

	// 2. Control: a wrong static password must be rejected, proving auth is enforced.
	bad, err := connector.NewConnector(model.RedisV9Type,
		model.WithRedisConfig(&model.RedisOptions{Addrs: []string{addr}, Password: "wrong-password"}),
	)
	require.NoError(t, err)
	assert.Error(t, bad.Ping(ctx), "wrong password must be rejected")
	_ = bad.Disconnect(ctx)

	// 3. Rotate the server password and the provider's value, then a NEW connector
	//    must authenticate with the rotated token — no static credential could.
	rotated := "tokenBBB-rotated"
	admin := redis.NewClient(&redis.Options{Addr: addr, Password: initialPass})
	require.NoError(t, admin.ConfigSet(ctx, "requirepass", rotated).Err())
	admin2 := redis.NewClient(&redis.Options{Addr: addr, Password: rotated})
	defer func() {
		// restore for any reruns
		_ = admin2.ConfigSet(ctx, "requirepass", initialPass).Err()
		_ = admin.Close()
		_ = admin2.Close()
	}()

	token.Store(rotated)
	conn2, err := connector.NewConnector(model.RedisV9Type,
		model.WithRedisConfig(&model.RedisOptions{Addrs: []string{addr}}),
		model.WithCredentialsProvider(provider),
	)
	require.NoError(t, err)
	require.NoError(t, conn2.Ping(ctx), "rotated token from provider should authenticate")
	require.NoError(t, conn2.Disconnect(ctx))
}
