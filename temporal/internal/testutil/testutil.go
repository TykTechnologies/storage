package testutil

import (
	"context"
	"log"
	"os"
	"testing"

	"github.com/TykTechnologies/storage/temporal/connector"
	"github.com/TykTechnologies/storage/temporal/model"
	"github.com/stretchr/testify/assert"
)

type StubConnector struct{}

func (m *StubConnector) Type() string {
	return "mock"
}

func (m *StubConnector) Connect(ctx context.Context) error {
	return nil
}

func (m *StubConnector) Disconnect(ctx context.Context) error {
	return nil
}

func (m *StubConnector) Ping(ctx context.Context) error {
	return nil
}

func (m *StubConnector) As(i interface{}) bool {
	return false
}

// Connectors returns a list of connectors to be used in tests.
// If you are adding a new supported driver, add it here and it will be tested on all the tcs automatically.
func TestConnectors(t *testing.T) []model.Connector {
	t.Helper()

	connectors := []model.Connector{}

	// redisv9 list
	redisConnector := newRedisConnector(t)

	connectors = append(connectors, redisConnector)

	return connectors
}

// IsClusterMode reports whether the temporal tests are running against a Redis
// Cluster (TEST_ENABLE_CLUSTER=true) rather than a single node.
//
// It exists so tests can account for cluster-only semantics. Notably, PUBLISH's
// reply counts only subscribers connected to the same node as the publisher;
// go-redis v9.11+ treats PUBLISH as keyless and routes it to a random node
// (while subscribers bind to the channel's slot node), so with subscribers
// present that count is non-deterministic even though delivery is unaffected.
func IsClusterMode() bool {
	return os.Getenv("TEST_ENABLE_CLUSTER") == "true"
}

func newRedisConnector(t *testing.T) model.Connector {
	t.Helper()

	addrs := []string{}

	addrsEnv := os.Getenv("TEST_REDIS_ADDRS")
	if addrsEnv == "" {
		log.Println("TEST_REDIS_ADDRS not set, using default localhost:6379")

		addrsEnv = "localhost:6379"
	}

	addrs = append(addrs, addrsEnv)

	enableCluster := IsClusterMode()
	if enableCluster {
		log.Println("TEST_ENABLE_CLUSTER is set, using cluster mode")
	}

	var tlsConfig *model.TLS
	if os.Getenv("TEST_ENABLE_TLS") == "true" {
		tlsConfig = &model.TLS{} // initializing with zero values
		tlsConfig.Enable = true

		tlsConfig.CertFile = os.Getenv("TEST_TLS_CERT_FILE")
		tlsConfig.KeyFile = os.Getenv("TEST_TLS_KEY_FILE")
		tlsConfig.CAFile = os.Getenv("TEST_TLS_CA_FILE")
		tlsConfig.InsecureSkipVerify = os.Getenv("TEST_TLS_INSECURE_SKIP_VERIFY") == "true"
	}

	redisConnector, err := connector.NewConnector(
		"redisv9", model.WithRedisConfig(&model.RedisOptions{Addrs: addrs, EnableCluster: enableCluster}),
		model.WithTLS(tlsConfig))
	assert.Nil(t, err)

	return redisConnector
}

func CloseConnectors(t *testing.T, connectors []model.Connector) {
	t.Helper()

	for _, connector := range connectors {
		if err := connector.Ping(context.Background()); err != nil {
			// Connector is already closed.
			continue
		}
		err := connector.Disconnect(context.Background())
		assert.Nil(t, err)
	}
}
