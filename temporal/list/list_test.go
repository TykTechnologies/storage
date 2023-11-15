package list

import (
	"context"
	"testing"

	"github.com/TykTechnologies/storage/temporal/connector"
	connectorType "github.com/TykTechnologies/storage/temporal/connector/types"
	"github.com/stretchr/testify/assert"
)

// Connectors returns a list of connectors to be used in tests.
func Connectors(t *testing.T) []connectorType.Connector {
	t.Helper()

	connectors := []connectorType.Connector{}

	// redisv8 list
	redisConnector, err := connector.NewConnector(
		"redisv8", connectorType.WithRedisConfig(&connectorType.RedisOptions{Addrs: []string{"localhost:6379"}}))
	assert.Nil(t, err)

	connectors = append(connectors, redisConnector)

	return connectors
}

func TestList_Range(t *testing.T) {
	connectors := Connectors(t)
	tcs := []struct {
		name                 string
		givenKey             string
		givenStart           int64
		givenStop            int64
		givenPreloadedValues [][]byte
		expectedErr          error
		expectedList         []string
	}{
		{
			name:                 "range_empty_values",
			givenKey:             "range_empty_values",
			givenPreloadedValues: [][]byte{},
			givenStart:           0,
			givenStop:            -1,
			expectedErr:          nil,
			expectedList:         []string{},
		},
		{
			name:                 "range_with_key",
			givenKey:             "range_with_key",
			givenPreloadedValues: [][]byte{[]byte("value1"), []byte("value2"), []byte("value2")},
			givenStart:           0,
			givenStop:            -1,
			expectedErr:          nil,
			expectedList:         []string{"value1", "value2", "value2"},
		},
		{
			name:                 "range_with_key_and_start_stop",
			givenKey:             "range_with_key_and_start_stop",
			givenPreloadedValues: [][]byte{[]byte("value1"), []byte("value2"), []byte("value2")},
			givenStart:           1,
			givenStop:            2,
			expectedErr:          nil,
			expectedList:         []string{"value2", "value2"},
		},
	}

	for _, connector := range connectors {
		for _, tc := range tcs {
			t.Run(connector.Type()+"_"+tc.name, func(t *testing.T) {
				list, err := NewList(connector)
				assert.Nil(t, err)

				ctx := context.Background()
				err = list.Append(ctx, true, tc.givenKey, tc.givenPreloadedValues...)
				assert.Nil(t, err)

				actualList, err := list.Range(context.Background(), tc.givenKey, tc.givenStart, tc.givenStop)
				assert.Equal(t, tc.expectedErr, err)
				assert.Equal(t, tc.expectedList, actualList)

			})
		}
	}
}
