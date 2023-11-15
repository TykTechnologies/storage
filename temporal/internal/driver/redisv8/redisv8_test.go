package redisv8

import (
	"context"
	"testing"

	"github.com/TykTechnologies/storage/temporal/connector"
	connectorType "github.com/TykTechnologies/storage/temporal/connector/types"
	"github.com/stretchr/testify/assert"
)

func getTestAddr(t *testing.T) string {
	t.Helper()
	return "localhost:6379"
}

func deferDisconnect(t *testing.T, connector connectorType.Connector) {
	t.Helper()

	err := connector.Disconnect(context.Background())
	assert.Nil(t, err, "error disconnecting from connector")
}

func testList(t *testing.T) (*RedisV8, connectorType.Connector) {
	t.Helper()

	connector, err := connector.NewConnector(
		"redisv8", connectorType.WithRedisConfig(&connectorType.RedisOptions{Addrs: []string{getTestAddr(t)}}))
	assert.Nil(t, err)

	list, err := NewRedisV8(connector)
	assert.Nil(t, err)

	return list, connector
}

func TestRedisV8List_AddingElements(t *testing.T) {
	list, connector := testList(t)
	defer deferDisconnect(t, connector)

	tcs := []struct {
		name          string
		key           string
		values        [][]byte
		expectedOrder []string
		prepend       bool // append by default
		expectedErr   error
		pipelined     bool
	}{
		{
			name:          "prepend_empty_key",
			key:           "",
			values:        [][]byte{[]byte("value1"), []byte("value2")},
			expectedOrder: []string{"value2", "value1"},
			prepend:       true,
			expectedErr:   nil,
		},
		{
			name:          "prepend_empty_values",
			key:           "key",
			values:        [][]byte{},
			expectedOrder: []string{},
			prepend:       true,
			expectedErr:   nil,
		},
		{
			name:          "prepend_with_key_and_values",
			key:           "key_new",
			values:        [][]byte{[]byte("value1"), []byte("value2")},
			expectedOrder: []string{"value2", "value1"},
			prepend:       true,
			expectedErr:   nil,
		},
		{
			name:          "prepend_with_key_and_values_pipelined",
			key:           "key_new_pipelined",
			values:        [][]byte{[]byte("value1"), []byte("value2")},
			expectedOrder: []string{"value2", "value1"},
			expectedErr:   nil,
			prepend:       true,
			pipelined:     true,
		},

		{
			name:          "append_empty_key",
			key:           "",
			values:        [][]byte{[]byte("value1"), []byte("value2")},
			expectedOrder: []string{"value1", "value2"},
			prepend:       false,
			expectedErr:   nil,
		},
		{
			name:          "append_empty_values",
			key:           "key",
			values:        [][]byte{},
			expectedOrder: []string{},
			prepend:       false,
			expectedErr:   nil,
		},
		{
			name:          "append_with_key_and_values",
			key:           "key_new",
			values:        [][]byte{[]byte("value1"), []byte("value2")},
			expectedOrder: []string{"value1", "value2"},
			prepend:       false,
			expectedErr:   nil,
		},
		{
			name:          "append_with_key_and_values_pipelined",
			key:           "key_new_pipelined",
			values:        [][]byte{[]byte("value1"), []byte("value2")},
			expectedOrder: []string{"value1", "value2"},
			expectedErr:   nil,
			prepend:       false,
			pipelined:     true,
		},
	}

	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			var err error
			if tc.prepend {
				err = list.Prepend(context.Background(), tc.pipelined, tc.key, tc.values...)
			} else {
				err = list.Append(context.Background(), tc.pipelined, tc.key, tc.values...)
			}
			assert.Equal(t, tc.expectedErr, err)

			defer list.client.FlushAll(context.Background())

			if tc.expectedErr == nil {
				assert.Equal(t, tc.expectedOrder, list.client.LRange(context.Background(), tc.key, 0, -1).Val())
			}
		})
	}
}

func TestRedisV8List_Remove(t *testing.T) {
	list, connector := testList(t)
	defer deferDisconnect(t, connector)

	tcs := []struct {
		name            string
		key             string
		givenValues     [][]byte
		givenCount      int64
		givenElement    []byte
		expectedErr     error
		expectedList    []string
		expectedDeleted int64
	}{
		{
			name:            "remove_empty_key",
			key:             "",
			givenValues:     [][]byte{[]byte("value1"), []byte("value2")},
			givenCount:      0,
			givenElement:    []byte("value1"),
			expectedErr:     nil,
			expectedList:    []string{"value2"},
			expectedDeleted: 1,
		},
		{
			name:            "remove_empty_values",
			key:             "key",
			givenValues:     [][]byte{[]byte("value1"), []byte("value2")},
			givenCount:      0,
			givenElement:    []byte{},
			expectedErr:     nil,
			expectedList:    []string{"value1", "value2"},
			expectedDeleted: 0,
		},
		{
			name:            "remove_with_key",
			key:             "key",
			givenValues:     [][]byte{[]byte("value1"), []byte("value2")},
			givenCount:      1,
			givenElement:    []byte("value1"),
			expectedErr:     nil,
			expectedList:    []string{"value2"},
			expectedDeleted: 1,
		},

		{
			name:            "remove_multiple_values",
			key:             "key",
			givenValues:     [][]byte{[]byte("value1"), []byte("value2"), []byte("value2")},
			givenCount:      0,
			givenElement:    []byte("value2"),
			expectedErr:     nil,
			expectedList:    []string{"value1"},
			expectedDeleted: 2,
		},
	}

	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			err := list.Append(context.Background(), false, tc.key, tc.givenValues...)
			assert.Nil(t, err)

			defer list.client.FlushAll(context.Background())

			actualDeleted, err := list.Remove(context.Background(), tc.key, tc.givenCount, tc.givenElement)
			assert.Equal(t, tc.expectedErr, err)
			assert.Equal(t, tc.expectedDeleted, actualDeleted)

			if err == nil {
				assert.Equal(t, tc.expectedList, list.client.LRange(context.Background(), tc.key, 0, -1).Val())
			}
		})
	}
}

func TestRedisV8List_Len(t *testing.T) {
	list, connector := testList(t)
	defer deferDisconnect(t, connector)

	tcs := []struct {
		name        string
		key         string
		givenValues [][]byte
		expectedErr error
		expectedLen int64
	}{
		{
			name:        "len_empty_key",
			key:         "",
			givenValues: [][]byte{[]byte("value1"), []byte("value2")},
			expectedErr: nil,
			expectedLen: 2,
		},
		{
			name:        "len_empty_values",
			key:         "key",
			givenValues: [][]byte{},
			expectedErr: nil,
			expectedLen: 0,
		},
		{
			name:        "len_with_key",
			key:         "",
			givenValues: [][]byte{[]byte("value1"), []byte("value2"), []byte("value2")},
			expectedErr: nil,
			expectedLen: 3,
		},
	}

	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			err := list.Append(context.Background(), false, tc.key, tc.givenValues...)
			assert.Nil(t, err)
			defer list.client.FlushAll(context.Background())

			actualLen, err := list.Length(context.Background(), tc.key)
			assert.Equal(t, tc.expectedErr, err)
			assert.Equal(t, tc.expectedLen, actualLen)
		})
	}
}

func TestRedisV8List_Range(t *testing.T) {
	list, connector := testList(t)
	defer deferDisconnect(t, connector)

	tcs := []struct {
		name         string
		key          string
		givenValues  [][]byte
		givenStart   int64
		givenStop    int64
		expectedErr  error
		expectedList []string
	}{
		{
			name:         "range_empty_key",
			key:          "",
			givenValues:  [][]byte{[]byte("value1"), []byte("value2")},
			givenStart:   0,
			givenStop:    -1,
			expectedErr:  nil,
			expectedList: []string{"value1", "value2"},
		},
		{
			name:         "range_empty_values",
			key:          "key",
			givenValues:  [][]byte{},
			givenStart:   0,
			givenStop:    -1,
			expectedErr:  nil,
			expectedList: []string{},
		},
		{
			name:         "range_with_key",
			key:          "key",
			givenValues:  [][]byte{[]byte("value1"), []byte("value2"), []byte("value2")},
			givenStart:   0,
			givenStop:    -1,
			expectedErr:  nil,
			expectedList: []string{"value1", "value2", "value2"},
		},
		{
			name:         "range_with_key_and_start_stop",
			key:          "key",
			givenValues:  [][]byte{[]byte("value1"), []byte("value2"), []byte("value2")},
			givenStart:   1,
			givenStop:    2,
			expectedErr:  nil,
			expectedList: []string{"value2", "value2"},
		},
	}

	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			err := list.Append(context.Background(), false, tc.key, tc.givenValues...)
			assert.Nil(t, err)
			defer list.client.FlushAll(context.Background())

			actualList, err := list.Range(context.Background(), tc.key, tc.givenStart, tc.givenStop)
			assert.Equal(t, tc.expectedErr, err)
			assert.Equal(t, tc.expectedList, actualList)
		})
	}
}

func TestRedisV8List_Pop(t *testing.T) {
	list, connector := testList(t)
	defer deferDisconnect(t, connector)

	tcs := []struct {
		name         string
		key          string
		givenValues  [][]byte
		givenStop    int64
		expectedErr  error
		expectedList []string
		expectedPop  []string
	}{
		{
			name:         "pop_all",
			key:          "key",
			givenValues:  [][]byte{[]byte("value1"), []byte("value2")},
			givenStop:    -1,
			expectedErr:  nil,
			expectedPop:  []string{"value1", "value2"},
			expectedList: []string{},
		},
		{
			name:         "pop_some_elements",
			key:          "key2",
			givenValues:  [][]byte{[]byte("value1"), []byte("value2"), []byte("value3"), []byte("value4")},
			givenStop:    2,
			expectedErr:  nil,
			expectedPop:  []string{"value1", "value2"},
			expectedList: []string{"value3", "value4"},
		},
	}

	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			err := list.Append(context.Background(), false, tc.key, tc.givenValues...)
			assert.Nil(t, err)
			defer list.client.FlushAll(context.Background())

			actualPop, err := list.Pop(context.Background(), tc.key, tc.givenStop)
			assert.Equal(t, tc.expectedErr, err)
			assert.Equal(t, tc.expectedPop, actualPop, "Pop elements differ from expected elements")

			actualElements := list.client.LRange(context.Background(), tc.key, 0, -1).Val()
			assert.Equal(t, tc.expectedList, actualElements, "Actual elements differ from expected elements")
		})
	}
}
