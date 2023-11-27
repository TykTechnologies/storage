package testutil

import (
	"context"
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

	// redisv8 list
	redisConnector, err := connector.NewConnector(
		"redisv8", model.WithRedisConfig(&model.RedisOptions{Addrs: []string{"localhost:6379"}}))
	assert.Nil(t, err)

	connectors = append(connectors, redisConnector)

	return connectors
}

func CloseConnectors(t *testing.T, connectors []model.Connector) {
	t.Helper()

	for _, connector := range connectors {
		err := connector.Disconnect(context.Background())
		assert.Nil(t, err)
	}
}
