package list

import (
	"context"
	"testing"

	"github.com/TykTechnologies/storage/temporal/flusher"
	"github.com/TykTechnologies/storage/temporal/internal/testutil"
	"github.com/stretchr/testify/assert"
)

func TestList_Range(t *testing.T) {
	connectors := testutil.TestConnectors(t)
	defer testutil.CloseConnectors(t, connectors)

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
				ctx := context.Background()

				list, err := NewList(connector)
				assert.Nil(t, err)

				flusher, err := flusher.NewFlusher(connector)
				assert.Nil(t, err)
				defer assert.Nil(t, flusher.FlushAll(ctx))

				err = list.Append(ctx, true, tc.givenKey, tc.givenPreloadedValues...)
				assert.Nil(t, err)

				actualList, err := list.Range(context.Background(), tc.givenKey, tc.givenStart, tc.givenStop)
				assert.Equal(t, tc.expectedErr, err)
				assert.Equal(t, tc.expectedList, actualList)
			})
		}
	}
}

func TestList_AddingElements(t *testing.T) {
	connectors := testutil.TestConnectors(t)
	defer testutil.CloseConnectors(t, connectors)

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

	for _, connector := range connectors {
		for _, tc := range tcs {
			t.Run(connector.Type()+"_"+tc.name, func(t *testing.T) {
				ctx := context.Background()

				list, err := NewList(connector)
				assert.Nil(t, err)

				flusher, err := flusher.NewFlusher(connector)
				assert.Nil(t, err)
				defer assert.Nil(t, flusher.FlushAll(ctx))

				if tc.prepend {
					err = list.Prepend(context.Background(), tc.pipelined, tc.key, tc.values...)
				} else {
					err = list.Append(context.Background(), tc.pipelined, tc.key, tc.values...)
				}
				assert.Equal(t, tc.expectedErr, err)

				if tc.expectedErr == nil {
					actualData, err := list.Range(ctx, tc.key, 0, -1)
					assert.Nil(t, err)
					assert.Equal(t, tc.expectedOrder, actualData)
				}
			})
		}
	}
}

func TestList_Remove(t *testing.T) {
	connectors := testutil.TestConnectors(t)
	defer testutil.CloseConnectors(t, connectors)

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

	for _, connector := range connectors {
		for _, tc := range tcs {
			t.Run(connector.Type()+"_"+tc.name, func(t *testing.T) {
				ctx := context.Background()

				list, err := NewList(connector)
				assert.Nil(t, err)

				flusher, err := flusher.NewFlusher(connector)
				assert.Nil(t, err)
				defer assert.Nil(t, flusher.FlushAll(ctx))

				err = list.Append(context.Background(), false, tc.key, tc.givenValues...)
				assert.Nil(t, err)

				assert.Equal(t, tc.expectedErr, err)

				actualDeleted, err := list.Remove(context.Background(), tc.key, tc.givenCount, tc.givenElement)
				assert.Equal(t, tc.expectedErr, err)
				assert.Equal(t, tc.expectedDeleted, actualDeleted)

				if err == nil {
					actualData, err := list.Range(ctx, tc.key, 0, -1)
					assert.Nil(t, err)
					assert.Equal(t, tc.expectedList, actualData)
				}
			})
		}
	}
}

func TestList_Len(t *testing.T) {
	connectors := testutil.TestConnectors(t)
	defer testutil.CloseConnectors(t, connectors)

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

	for _, connector := range connectors {
		for _, tc := range tcs {
			t.Run(connector.Type()+"_"+tc.name, func(t *testing.T) {
				ctx := context.Background()

				list, err := NewList(connector)
				assert.Nil(t, err)

				flusher, err := flusher.NewFlusher(connector)
				assert.Nil(t, err)
				defer assert.Nil(t, flusher.FlushAll(ctx))

				err = list.Append(context.Background(), false, tc.key, tc.givenValues...)
				assert.Equal(t, tc.expectedErr, err)

				actualLen, err := list.Length(context.Background(), tc.key)
				assert.Equal(t, tc.expectedErr, err)
				assert.Equal(t, tc.expectedLen, actualLen)
			})
		}
	}
}

func TestList_Pop(t *testing.T) {
	connectors := testutil.TestConnectors(t)
	defer testutil.CloseConnectors(t, connectors)

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

	for _, connector := range connectors {
		for _, tc := range tcs {
			t.Run(connector.Type()+"_"+tc.name, func(t *testing.T) {
				ctx := context.Background()

				list, err := NewList(connector)
				assert.Nil(t, err)

				flusher, err := flusher.NewFlusher(connector)
				assert.Nil(t, err)
				defer assert.Nil(t, flusher.FlushAll(ctx))

				err = list.Append(context.Background(), false, tc.key, tc.givenValues...)
				assert.Nil(t, err)

				actualPop, err := list.Pop(context.Background(), tc.key, tc.givenStop)
				assert.Equal(t, tc.expectedErr, err)
				assert.Equal(t, tc.expectedPop, actualPop, "Pop elements differ from expected elements")

				actualElements, err := list.Range(context.Background(), tc.key, 0, -1)
				assert.Nil(t, err)
				assert.Equal(t, tc.expectedList, actualElements, "Actual elements differ from expected elements")
			})
		}
	}
}
