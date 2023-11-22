package flusher

import (
	"context"
	"testing"

	"github.com/TykTechnologies/storage/temporal/internal/testutil"
	"github.com/stretchr/testify/assert"
)

func TestNewFlusher(t *testing.T) {
	connectors := testutil.TestConnectors(t)
	defer testutil.CloseConnectors(t, connectors)

	for _, connector := range connectors {
		flusher, err := NewFlusher(connector)
		assert.Nil(t, err)
		assert.NotNil(t, flusher)
	}
}

func TestFlusher_FlushAll(t *testing.T) {
	connectors := testutil.TestConnectors(t)
	defer testutil.CloseConnectors(t, connectors)

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
