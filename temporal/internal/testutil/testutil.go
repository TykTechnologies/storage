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

// Connectors returns a list of connectors to be used in tests.
// If you are adding a new supported driver, add it here and it will be tested on all the tcs automatically.
func TestConnectors(t *testing.T) []model.Connector {
	t.Helper()

	connectors := []model.Connector{}

	// redisv8 list
	redisConnector := newRedisConnector(t)

	connectors = append(connectors, redisConnector)

	return connectors
}

func newRedisConnector(t *testing.T) model.Connector {
	t.Helper()

	addrs := []string{}

	addrsEnv := os.Getenv("REDIS_ADDRS")
	if addrsEnv == "" {
		log.Println("REDIS_ADDRS not set, using default localhost:6379")

		addrsEnv = "localhost:6379"
	}

	enableCluster := false
	enableClusterEnv := os.Getenv("REDIS_ENABLE_CLUSTER")
	if enableClusterEnv != "" {
		log.Println("REDIS_ENABLE_CLUSTER is set, using cluster mode")
		enableCluster = true
	} else {
		log.Println("REDIS_ENABLE_CLUSTER not set, using standalone mode")
	}

	redisConnector, err := connector.NewConnector(
		"redisv8", model.WithRedisConfig(&model.RedisOptions{Addrs: addrs, EnableCluster: enableCluster}))
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
