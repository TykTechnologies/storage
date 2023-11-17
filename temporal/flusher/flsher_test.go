package flusher

import (
	"context"
	"testing"

	"github.com/TykTechnologies/storage/temporal/connector"
	"github.com/TykTechnologies/storage/temporal/model"
	"github.com/stretchr/testify/assert"
)

func testConnectors(t *testing.T) []model.Connector {
	t.Helper()

	connectors := []model.Connector{}

	// redisv8 list
	redisConnector, err := connector.NewConnector(
		"redisv8", model.WithRedisConfig(&model.RedisOptions{Addrs: []string{"localhost:6379"}}))
	assert.Nil(t, err)

	connectors = append(connectors, redisConnector)

	return connectors
}

func closeConnectors(t *testing.T, connectors []model.Connector) {
	t.Helper()

	for _, connector := range connectors {
		err := connector.Disconnect(context.Background())
		assert.Nil(t, err)
	}
}

func TestNewFlusher(t *testing.T) {
	connectors := testConnectors(t)
	defer closeConnectors(t, connectors)

	for _, connector := range connectors {
		flusher, err := NewFlusher(connector)
		assert.Nil(t, err)
		assert.NotNil(t, flusher)
	}
}

func TestFlusher_FlushAll(t *testing.T) {
	connectors := testConnectors(t)
	defer closeConnectors(t, connectors)

	for _, connector := range connectors {
		t.Run(connector.Type(), func(t *testing.T) {
			flusher, err := NewFlusher(connector)
			assert.Nil(t, err)
			assert.NotNil(t, flusher)

			err = flusher.FlushAll(context.Background())
			assert.Nil(t, err)
		})
	}
}
