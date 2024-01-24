package temporal

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/TykTechnologies/storage/temporal/flusher"
	"github.com/TykTechnologies/storage/temporal/internal/testutil"
	"github.com/TykTechnologies/storage/temporal/model"
	"github.com/TykTechnologies/storage/temporal/temperr"
	"github.com/stretchr/testify/assert"
)

func TestKeyValue_Set(t *testing.T) {
	connectors := testutil.TestConnectors(t)
	defer testutil.CloseConnectors(t, connectors)

	tcs := []struct {
		name        string
		key         string
		value       string
		expiration  time.Duration
		expectedErr error
	}{
		{
			name:        "set_with_valid_key_and_value",
			key:         "key1",
			value:       "value1",
			expiration:  10 * time.Second,
			expectedErr: nil,
		},
		{
			name:        "set_with_empty_key",
			key:         "",
			value:       "value2",
			expiration:  10 * time.Second,
			expectedErr: temperr.KeyEmpty,
		},
		{
			name:        "set_with_empty_value",
			key:         "key3",
			value:       "",
			expiration:  10 * time.Second,
			expectedErr: nil,
		},
		{
			name:        "set_with_no_expiration",
			key:         "key4",
			value:       "value4",
			expiration:  10 * time.Second,
			expectedErr: nil,
		},
	}

	for _, connector := range connectors {
		for _, tc := range tcs {
			t.Run(connector.Type()+"_"+tc.name, func(t *testing.T) {
				ctx := context.Background()

				kv, err := NewKeyValue(connector)
				assert.Nil(t, err)

				flusher, err := flusher.NewFlusher(connector)
				assert.Nil(t, err)
				defer assert.Nil(t, flusher.FlushAll(ctx))

				err = kv.Set(ctx, tc.key, tc.value, tc.expiration)
				assert.Equal(t, tc.expectedErr, err)

				if err == nil {
					actualValue, err := kv.Get(ctx, tc.key)
					assert.Nil(t, err)

					assert.Equal(t, tc.value, actualValue)

					actualTTL, err := kv.TTL(ctx, tc.key)
					assert.Nil(t, err)
					assert.True(t, actualTTL <= int64(tc.expiration.Seconds()))
				}
			})
		}
	}
}

func TestKeyValue_Get(t *testing.T) {
	connectors := testutil.TestConnectors(t)
	defer testutil.CloseConnectors(t, connectors)

	tcs := []struct {
		name          string
		setup         func(db KeyValue)
		key           string
		expectedValue string
		expectedErr   error
	}{
		{
			name:          "non_existing_key",
			key:           "key1",
			expectedValue: "",
			expectedErr:   temperr.KeyNotFound,
		},
		{
			name: "existing_key",
			setup: func(rdb KeyValue) {
				err := rdb.Set(context.Background(), "key2", "value2", 0)
				if err != nil {
					t.Fatalf("Set() error = %v", err)
				}
			},
			key:           "key2",
			expectedValue: "value2",
			expectedErr:   nil,
		},
		{
			name:          "empty_key",
			key:           "",
			expectedValue: "",
			expectedErr:   temperr.KeyEmpty,
		},
	}

	for _, connector := range connectors {
		for _, tc := range tcs {
			t.Run(connector.Type()+"_"+tc.name, func(t *testing.T) {
				ctx := context.Background()

				kv, err := NewKeyValue(connector)
				assert.Nil(t, err)

				flusher, err := flusher.NewFlusher(connector)
				assert.Nil(t, err)
				defer assert.Nil(t, flusher.FlushAll(ctx))

				if tc.setup != nil {
					tc.setup(kv)
				}

				got, err := kv.Get(ctx, tc.key)
				assert.Equal(t, tc.expectedErr, err)
				assert.Equal(t, tc.expectedValue, got)
			})
		}
	}
}

func TestKeyValue_Delete(t *testing.T) {
	connectors := testutil.TestConnectors(t)
	defer testutil.CloseConnectors(t, connectors)

	tcs := []struct {
		name        string
		setup       func(db KeyValue)
		key         string
		expectedErr error
	}{
		{
			name: "existing_key",
			setup: func(db KeyValue) {
				err := db.Set(context.Background(), "key1", "value1", 0)
				if err != nil {
					t.Fatalf("Set() error = %v", err)
				}
			},
			key:         "key1",
			expectedErr: nil,
		},
		{
			name:        "non_existing_key",
			key:         "key2",
			expectedErr: nil,
		},
		{
			name:        "empty_key",
			key:         "",
			expectedErr: temperr.KeyEmpty,
		},
	}

	for _, connector := range connectors {
		for _, tc := range tcs {
			t.Run(connector.Type()+"_"+tc.name, func(t *testing.T) {
				ctx := context.Background()

				kv, err := NewKeyValue(connector)
				assert.Nil(t, err)

				flusher, err := flusher.NewFlusher(connector)
				assert.Nil(t, err)
				defer assert.Nil(t, flusher.FlushAll(ctx))

				if tc.setup != nil {
					tc.setup(kv)
				}

				err = kv.Delete(ctx, tc.key)
				assert.Equal(t, tc.expectedErr, err)

				if err == nil {
					_, err := kv.Get(ctx, tc.key)
					assert.Equal(t, temperr.KeyNotFound, err)
				}
			})
		}
	}
}

func TestKeyValue_Increment(t *testing.T) {
	connectors := testutil.TestConnectors(t)
	defer testutil.CloseConnectors(t, connectors)

	tcs := []struct {
		name          string
		setup         func(db KeyValue)
		key           string
		expectedValue int64
		expectedErr   error
	}{
		{
			name: "existing_key",
			setup: func(db KeyValue) {
				err := db.Set(context.Background(), "counter", "5", 0)
				if err != nil {
					t.Fatalf("Set() error = %v", err)
				}
			},
			key:           "counter",
			expectedValue: 6,
			expectedErr:   nil,
		},
		{
			name:          "non_existing_key",
			key:           "counter",
			expectedValue: 1,
			expectedErr:   nil,
		},
		{
			name:          "empty_key",
			key:           "",
			expectedValue: 0,
			expectedErr:   temperr.KeyEmpty,
		},
		{
			name: "string_key_value",
			setup: func(db KeyValue) {
				err := db.Set(context.Background(), "counter", "test", 0)
				if err != nil {
					t.Fatalf("Set() error = %v", err)
				}
			},
			key:           "counter",
			expectedValue: 0,
			expectedErr:   temperr.KeyMisstype,
		},
	}

	for _, connector := range connectors {
		for _, tc := range tcs {
			t.Run(connector.Type()+"_"+tc.name, func(t *testing.T) {
				ctx := context.Background()

				kv, err := NewKeyValue(connector)
				assert.Nil(t, err)

				flusher, err := flusher.NewFlusher(connector)
				assert.Nil(t, err)
				defer assert.Nil(t, flusher.FlushAll(ctx))

				if tc.setup != nil {
					tc.setup(kv)
				}

				got, err := kv.Increment(ctx, tc.key)
				assert.Equal(t, tc.expectedErr, err)
				assert.Equal(t, tc.expectedValue, got)
			})
		}
	}
}

func TestKeyValue_Decrement(t *testing.T) {
	connectors := testutil.TestConnectors(t)
	defer testutil.CloseConnectors(t, connectors)

	tcs := []struct {
		name          string
		setup         func(db KeyValue)
		key           string
		expectedValue int64
		expectedErr   error
	}{
		{
			name: "existing_key",
			setup: func(db KeyValue) {
				err := db.Set(context.Background(), "counter", "5", 0)
				if err != nil {
					t.Fatalf("Set() error = %v", err)
				}
			},
			key:           "counter",
			expectedValue: 4,
			expectedErr:   nil,
		},
		{
			name:          "non_existing_key",
			key:           "counter",
			expectedValue: -1,
			expectedErr:   nil,
		},
		{
			name:          "empty_key",
			key:           "",
			expectedValue: 0,
			expectedErr:   temperr.KeyEmpty,
		},
		{
			name: "string_key_value",
			setup: func(db KeyValue) {
				err := db.Set(context.Background(), "counter", "test", 0)
				if err != nil {
					t.Fatalf("Set() error = %v", err)
				}
			},
			key:           "counter",
			expectedValue: 0,
			expectedErr:   temperr.KeyMisstype,
		},
	}

	for _, connector := range connectors {
		for _, tc := range tcs {
			t.Run(connector.Type()+"_"+tc.name, func(t *testing.T) {
				ctx := context.Background()

				kv, err := NewKeyValue(connector)
				assert.Nil(t, err)

				flusher, err := flusher.NewFlusher(connector)
				assert.Nil(t, err)
				defer assert.Nil(t, flusher.FlushAll(ctx))

				if tc.setup != nil {
					tc.setup(kv)
				}

				got, err := kv.Decrement(ctx, tc.key)
				assert.Equal(t, tc.expectedErr, err)
				assert.Equal(t, tc.expectedValue, got)
			})
		}
	}
}

func TestKeyValue_Exist(t *testing.T) {
	connectors := testutil.TestConnectors(t)
	defer testutil.CloseConnectors(t, connectors)

	tcs := []struct {
		name          string
		setup         func(db KeyValue)
		key           string
		expectedValue bool
		expectedErr   error
	}{
		{
			name: "existing_key",
			setup: func(db KeyValue) {
				err := db.Set(context.Background(), "key1", "value1", 0)
				if err != nil {
					t.Fatalf("Set() error = %v", err)
				}
			},
			key:           "key1",
			expectedValue: true,
			expectedErr:   nil,
		},
		{
			name:          "non_existing_key",
			key:           "key2",
			expectedValue: false,
			expectedErr:   nil,
		},
		{
			name:          "empty_key",
			key:           "",
			expectedValue: false,
			expectedErr:   temperr.KeyEmpty,
		},
	}

	for _, connector := range connectors {
		for _, tc := range tcs {
			t.Run(connector.Type()+"_"+tc.name, func(t *testing.T) {
				ctx := context.Background()

				kv, err := NewKeyValue(connector)
				assert.Nil(t, err)

				flusher, err := flusher.NewFlusher(connector)
				assert.Nil(t, err)
				defer assert.Nil(t, flusher.FlushAll(ctx))

				if tc.setup != nil {
					tc.setup(kv)
				}

				got, err := kv.Exists(ctx, tc.key)
				assert.Equal(t, tc.expectedErr, err)
				assert.Equal(t, tc.expectedValue, got)
			})
		}
	}
}

func TestKeyValue_Expire(t *testing.T) {
	connectors := testutil.TestConnectors(t)
	defer testutil.CloseConnectors(t, connectors)

	tcs := []struct {
		name        string
		setup       func(db KeyValue)
		key         string
		expiration  time.Duration
		expectedErr error
	}{
		{
			name: "existing_key",
			setup: func(db KeyValue) {
				err := db.Set(context.Background(), "key1", "value1", 0)
				if err != nil {
					t.Fatalf("Set() error = %v", err)
				}
			},
			key: "key1",

			expiration:  10 * time.Second,
			expectedErr: nil,
		},
		{
			name:        "non_existing_key",
			key:         "key2",
			expiration:  10 * time.Second,
			expectedErr: nil,
		},
		{
			name:        "empty_key",
			key:         "",
			expiration:  10 * time.Second,
			expectedErr: temperr.KeyEmpty,
		},
	}

	for _, connector := range connectors {
		for _, tc := range tcs {
			t.Run(connector.Type()+"_"+tc.name, func(t *testing.T) {
				ctx := context.Background()

				kv, err := NewKeyValue(connector)
				assert.Nil(t, err)

				flusher, err := flusher.NewFlusher(connector)
				assert.Nil(t, err)
				defer assert.Nil(t, flusher.FlushAll(ctx))

				if tc.setup != nil {
					tc.setup(kv)
				}

				err = kv.Expire(ctx, tc.key, tc.expiration)
				assert.Equal(t, tc.expectedErr, err)
				if err == nil {
					actualTTL, err := kv.TTL(ctx, tc.key)
					assert.Nil(t, err)
					assert.True(t, actualTTL <= int64(tc.expiration.Seconds()))
				}
			})
		}
	}
}

func TestKeyValue_TTL(t *testing.T) {
	connectors := testutil.TestConnectors(t)
	defer testutil.CloseConnectors(t, connectors)

	tcs := []struct {
		name        string
		setup       func(db KeyValue)
		key         string
		expectedTTL int64
		expectedErr error
	}{
		{
			name: "existing_key",
			setup: func(db KeyValue) {
				err := db.Set(context.Background(), "key1", "value1", 10*time.Second)
				if err != nil {
					t.Fatalf("Set() error = %v", err)
				}
			},
			key:         "key1",
			expectedTTL: 10,
			expectedErr: nil,
		},
		{
			name: "existing_non_exipiring_key",
			setup: func(db KeyValue) {
				err := db.Set(context.Background(), "key2", "value1", -1*time.Second)
				if err != nil {
					t.Fatalf("Set() error = %v", err)
				}
			},
			key:         "key2",
			expectedTTL: -1,
			expectedErr: nil,
		},
		{
			name: "key_without_ttl",
			setup: func(db KeyValue) {
				err := db.Set(context.Background(), "key_without_ttl", "value1", 0*time.Second)
				if err != nil {
					t.Fatalf("Set() error = %v", err)
				}
			},
			key:         "key_without_ttl",
			expectedTTL: -1,
			expectedErr: nil,
		},
		{
			name:        "non_existing_key",
			key:         "non_existing_key",
			expectedTTL: -2,
			expectedErr: nil,
		},
		{
			name:        "empty_key",
			key:         "",
			expectedTTL: -2,
			expectedErr: temperr.KeyEmpty,
		},
	}

	for _, connector := range connectors {
		for _, tc := range tcs {
			t.Run(connector.Type()+"_"+tc.name, func(t *testing.T) {
				ctx := context.Background()

				kv, err := NewKeyValue(connector)
				assert.Nil(t, err)

				flusher, err := flusher.NewFlusher(connector)
				assert.Nil(t, err)
				defer assert.Nil(t, flusher.FlushAll(ctx))

				if tc.setup != nil {
					tc.setup(kv)
				}

				actualTTL, err := kv.TTL(ctx, tc.key)
				assert.Equal(t, tc.expectedErr, err)
				assert.Equal(t, tc.expectedTTL, actualTTL)
			})
		}
	}
}

func TestKeyValue_DeleteKeys(t *testing.T) {
	connectors := testutil.TestConnectors(t)
	defer testutil.CloseConnectors(t, connectors)

	tcs := []struct {
		name            string
		setup           func(db KeyValue)
		keys            []string
		expectedDeleted int64
		expectedErr     error
	}{
		{
			name: "existing_keys",
			setup: func(db KeyValue) {
				err := db.Set(context.Background(), "key1", "value1", 0)
				if err != nil {
					t.Fatalf("Set() error = %v", err)
				}
				err = db.Set(context.Background(), "key2", "value2", 0)
				if err != nil {
					t.Fatalf("Set() error = %v", err)
				}
				err = db.Set(context.Background(), "key3", "value3", 0)
				if err != nil {
					t.Fatalf("Set() error = %v", err)
				}
			},
			keys:            []string{"key1", "key2"},
			expectedDeleted: 2,
			expectedErr:     nil,
		},
		{
			name:            "non_existing_keys",
			keys:            []string{"key1", "key2"},
			expectedDeleted: 0,
			expectedErr:     nil,
		},
		{
			name:        "empty_keys",
			keys:        []string{},
			expectedErr: temperr.KeyEmpty,
		},
	}

	for _, connector := range connectors {
		for _, tc := range tcs {
			t.Run(connector.Type()+"_"+tc.name, func(t *testing.T) {
				ctx := context.Background()

				kv, err := NewKeyValue(connector)
				assert.Nil(t, err)

				flusher, err := flusher.NewFlusher(connector)
				assert.Nil(t, err)
				defer assert.Nil(t, flusher.FlushAll(ctx))

				if tc.setup != nil {
					tc.setup(kv)
				}

				deleted, err := kv.DeleteKeys(ctx, tc.keys)
				assert.Equal(t, tc.expectedErr, err)
				assert.Equal(t, tc.expectedDeleted, deleted)
			})
		}
	}
}

func TestKeyValue_DeleteScanMatch(t *testing.T) {
	connectors := testutil.TestConnectors(t)
	defer testutil.CloseConnectors(t, connectors)

	tcs := []struct {
		name            string
		setup           func(db KeyValue)
		pattern         string
		expectedDeleted int64
		expectedErr     error
	}{
		{
			name: "existing_keys_pattern",
			setup: func(db KeyValue) {
				err := db.Set(context.Background(), "key1", "value1", 0)
				if err != nil {
					t.Fatalf("Set() error = %v", err)
				}
				err = db.Set(context.Background(), "key2", "value2", 0)
				if err != nil {
					t.Fatalf("Set() error = %v", err)
				}
				err = db.Set(context.Background(), "test", "value2", 0)
				if err != nil {
					t.Fatalf("Set() error = %v", err)
				}
			},
			pattern:         "key*",
			expectedDeleted: 2,
			expectedErr:     nil,
		},
		{
			name:            "non_matching_pattern",
			pattern:         "key*",
			expectedDeleted: 0,
			expectedErr:     nil,
		},
		{
			name:        "empty_pattern",
			pattern:     "",
			expectedErr: nil,
		},
		{
			name:            "closed_connection",
			pattern:         "key*",
			expectedDeleted: 0,
			expectedErr:     temperr.ClosedConnection,
		},
	}

	for _, connector := range connectors {
		for _, tc := range tcs {
			t.Run(connector.Type()+"_"+tc.name, func(t *testing.T) {
				ctx := context.Background()

				kv, err := NewKeyValue(connector)
				assert.Nil(t, err)

				flusher, err := flusher.NewFlusher(connector)
				assert.Nil(t, err)

				if errors.Is(tc.expectedErr, temperr.ClosedConnection) {
					err := connector.Disconnect(ctx)
					assert.Nil(t, err)
				} else {
					defer assert.Nil(t, flusher.FlushAll(ctx))
				}

				if tc.setup != nil {
					tc.setup(kv)
				}

				deleted, err := kv.DeleteScanMatch(ctx, tc.pattern)
				assert.Equal(t, tc.expectedErr, err)
				assert.Equal(t, tc.expectedDeleted, deleted)
			})
		}
	}
}

func TestKeyValue_Keys(t *testing.T) {
	connectors := testutil.TestConnectors(t)
	defer testutil.CloseConnectors(t, connectors)

	tcs := []struct {
		name         string
		setup        func(db KeyValue)
		pattern      string
		expectedKeys []string
		expectedErr  error
	}{
		{
			name: "existing_keys_pattern",
			setup: func(db KeyValue) {
				err := db.Set(context.Background(), "key1", "value1", 0)
				if err != nil {
					t.Fatalf("Set() error = %v", err)
				}
				err = db.Set(context.Background(), "key2", "value2", 0)
				if err != nil {
					t.Fatalf("Set() error = %v", err)
				}
				err = db.Set(context.Background(), "test", "value2", 0)
				if err != nil {
					t.Fatalf("Set() error = %v", err)
				}
			},
			pattern:      "key*",
			expectedKeys: []string{"key1", "key2"},
			expectedErr:  nil,
		},
		{
			name:         "non_matching_pattern",
			pattern:      "key*",
			expectedKeys: []string{},
			expectedErr:  nil,
		},
		{
			name: "empty_pattern",
			setup: func(db KeyValue) {
				err := db.Set(context.Background(), "test2", "value1", 0)
				if err != nil {
					t.Fatalf("Set() error = %v", err)
				}
				err = db.Set(context.Background(), "test3", "value2", 0)
				if err != nil {
					t.Fatalf("Set() error = %v", err)
				}
			},
			pattern:      "",
			expectedKeys: []string{"test2", "test3"}, // SCAN will iterate over all the keys if no pattern provided.
			expectedErr:  nil,
		},
		{
			name:         "closed_connection",
			pattern:      "key*",
			expectedKeys: []string{},
			expectedErr:  temperr.ClosedConnection,
		},
	}

	for _, connector := range connectors {
		for _, tc := range tcs {
			t.Run(connector.Type()+"_"+tc.name, func(t *testing.T) {
				ctx := context.Background()

				kv, err := NewKeyValue(connector)
				assert.Nil(t, err)

				flusher, err := flusher.NewFlusher(connector)
				assert.Nil(t, err)

				if errors.Is(tc.expectedErr, temperr.ClosedConnection) {
					err := connector.Disconnect(ctx)
					assert.Nil(t, err)
				} else {
					defer assert.Nil(t, flusher.FlushAll(ctx))
				}

				if tc.setup != nil {
					tc.setup(kv)
				}

				keys, err := kv.Keys(ctx, tc.pattern)
				assert.Equal(t, tc.expectedErr, err)
				assert.ElementsMatch(t, tc.expectedKeys, keys)
			})
		}
	}
}

func TestKeyValue_GetMulti(t *testing.T) {
	connectors := testutil.TestConnectors(t)
	defer testutil.CloseConnectors(t, connectors)

	tcs := []struct {
		name           string
		setup          func(db KeyValue)
		keys           []string
		expectedValues []interface{}
		expectedErr    error
	}{
		{
			name: "existing_keys",
			setup: func(db KeyValue) {
				err := db.Set(context.Background(), "key1", "value1", 0)
				if err != nil {
					t.Fatalf("Set() error = %v", err)
				}
				err = db.Set(context.Background(), "key2", "value2", 0)
				if err != nil {
					t.Fatalf("Set() error = %v", err)
				}
				err = db.Set(context.Background(), "test", "value2", 0)
				if err != nil {
					t.Fatalf("Set() error = %v", err)
				}
			},
			keys:           []string{"key1", "key2"},
			expectedValues: []interface{}{"value1", "value2"},
			expectedErr:    nil,
		},
		{
			name:           "non_existing_keys",
			keys:           []string{"key1", "key2"},
			expectedValues: []interface{}{nil, nil},
			expectedErr:    nil,
		},
		{
			name: "mixed_existing_and_non_existing_keys",
			setup: func(db KeyValue) {
				err := db.Set(context.Background(), "key1", "value1", 0)
				if err != nil {
					t.Fatalf("Set() error = %v", err)
				}
				err = db.Set(context.Background(), "test", "value2", 0)
				if err != nil {
					t.Fatalf("Set() error = %v", err)
				}
			},
			keys:           []string{"key1", "key2"},
			expectedValues: []interface{}{"value1", nil},
			expectedErr:    nil,
		},
	}

	for _, connector := range connectors {
		for _, tc := range tcs {
			t.Run(connector.Type()+"_"+tc.name, func(t *testing.T) {
				ctx := context.Background()

				kv, err := NewKeyValue(connector)
				assert.Nil(t, err)

				flusher, err := flusher.NewFlusher(connector)
				assert.Nil(t, err)
				defer assert.Nil(t, flusher.FlushAll(ctx))

				if tc.setup != nil {
					tc.setup(kv)
				}

				values, err := kv.GetMulti(ctx, tc.keys)
				assert.Equal(t, tc.expectedErr, err)
				assert.ElementsMatch(t, tc.expectedValues, values)
			})
		}
	}
}

func TestKeyValue_GetKeysAndValuesWithFilter(t *testing.T) {
	connectors := testutil.TestConnectors(t)
	defer testutil.CloseConnectors(t, connectors)

	tcs := []struct {
		name           string
		setup          func(db KeyValue)
		pattern        string
		expectedValues map[string]interface{}
		expectedErr    error
	}{
		{
			name: "existing_keys_pattern",
			setup: func(db KeyValue) {
				err := db.Set(context.Background(), "key", "value", 0)
				if err != nil {
					t.Fatalf("Set() error = %v", err)
				}
				err = db.Set(context.Background(), "key2", "value2", 0)
				if err != nil {
					t.Fatalf("Set() error = %v", err)
				}
				err = db.Set(context.Background(), "test", "value2", 0)
				if err != nil {
					t.Fatalf("Set() error = %v", err)
				}
			},
			pattern:        "key*",
			expectedValues: map[string]interface{}{"key": "value", "key2": "value2"},
			expectedErr:    nil,
		},
		{
			name:           "non_matching_pattern",
			pattern:        "key*",
			expectedValues: map[string]interface{}{},
			expectedErr:    nil,
		},
		{
			name: "empty_pattern",
			setup: func(db KeyValue) {
				err := db.Set(context.Background(), "key1", "value1", 0)
				if err != nil {
					t.Fatalf("Set() error = %v", err)
				}
				err = db.Set(context.Background(), "key2", "value2", 0)
				if err != nil {
					t.Fatalf("Set() error = %v", err)
				}
				err = db.Set(context.Background(), "test", "value2", 0)
				if err != nil {
					t.Fatalf("Set() error = %v", err)
				}
			},
			pattern:        "",
			expectedValues: map[string]interface{}{"key1": "value1", "key2": "value2", "test": "value2"},
			expectedErr:    nil,
		},
	}

	for _, connector := range connectors {
		for _, tc := range tcs {
			t.Run(connector.Type()+"_"+tc.name, func(t *testing.T) {
				ctx := context.Background()

				kv, err := NewKeyValue(connector)
				assert.Nil(t, err)

				flusher, err := flusher.NewFlusher(connector)
				assert.Nil(t, err)

				defer assert.Nil(t, flusher.FlushAll(ctx))

				if tc.setup != nil {
					tc.setup(kv)
				}

				data, err := kv.GetKeysAndValuesWithFilter(ctx, tc.pattern)
				assert.Equal(t, tc.expectedErr, err)
				assert.Equal(t, tc.expectedValues, data)
			})
		}
	}
}

func TestKeyValue_GetKeysWithOpts(t *testing.T) {
	connectors := testutil.TestConnectors(t)
	defer testutil.CloseConnectors(t, connectors)

	tcs := []struct {
		name                string
		setup               func(model.KeyValue)
		searchStr           string
		cursor              map[string]uint64
		count               int64
		expectedKeysCheck   func([]string) bool
		expectedCursorCheck func(cursorMap map[string]uint64) bool
		continueScanCheck   bool
		expectedErr         error
		onlyCluster         bool // Only run certain checks on cluster connectors
	}{
		{
			name: "valid_search",
			setup: func(redis model.KeyValue) {
				ctx := context.Background()
				assert.NoError(t, redis.Set(ctx, "key2", "value2", 0))
				assert.NoError(t, redis.Set(ctx, "key1", "value1", 0))
			},
			searchStr: "key*",
			cursor:    nil,
			count:     10,
			expectedKeysCheck: func(s []string) bool {
				return len(s) == 2 && (s[0] == "key1" || s[0] == "key2") && (s[1] == "key1" || s[1] == "key2")
			},
			expectedCursorCheck: func(cursorMap map[string]uint64) bool {
				if len(cursorMap) == 0 {
					return false
				}

				for _, c := range cursorMap {
					if c != 0 {
						return false
					}
				}

				return true
			},
			expectedErr: nil,
		},
		{
			name:      "empty_search",
			setup:     nil,
			searchStr: "",
			cursor:    nil,
			count:     10,
			expectedKeysCheck: func(s []string) bool {
				return len(s) == 0
			},
			expectedCursorCheck: func(cursorMap map[string]uint64) bool {
				if len(cursorMap) == 0 {
					return false
				}

				for _, c := range cursorMap {
					if c != 0 {
						return false
					}
				}

				return true
			},
			expectedErr: nil,
		},
		{
			name: "specific_pattern_search",
			setup: func(kv model.KeyValue) {
				ctx := context.Background()
				assert.NoError(t, kv.Set(ctx, "specific1", "value1", 0))
				assert.NoError(t, kv.Set(ctx, "specific2", "value2", 0))
			},
			searchStr: "specific*",
			cursor:    nil,
			count:     10,
			expectedKeysCheck: func(s []string) bool {
				return len(s) == 2 &&
					(s[0] == "specific1" || s[0] == "specific2") &&
					(s[1] == "specific1" || s[1] == "specific2")
			},
			expectedCursorCheck: func(cursorMap map[string]uint64) bool {
				if len(cursorMap) == 0 {
					return false
				}

				for _, c := range cursorMap {
					if c != 0 {
						return false
					}
				}

				return true
			},
			expectedErr: nil,
		},
		{
			name:      "non_matching_pattern",
			setup:     nil,
			searchStr: "nomatch*",
			cursor:    nil,
			count:     10,
			expectedKeysCheck: func(s []string) bool {
				return len(s) == 0
			},
			expectedCursorCheck: func(cursorMap map[string]uint64) bool {
				if len(cursorMap) == 0 {
					return false
				}

				for _, c := range cursorMap {
					if c != 0 {
						return false
					}
				}

				return true
			},
			expectedErr: nil,
		},
		{
			name: "paginated_search",
			setup: func(kv model.KeyValue) {
				ctx := context.Background()
				for i := 0; i < 50; i++ {
					assert.NoError(t, kv.Set(ctx, fmt.Sprintf("pagekey%d", i), fmt.Sprintf("value%d", i), 0))
				}
			},
			searchStr: "pagekey*",
			cursor:    nil,
			count:     5, // Count is 1 but Redis SCAN does not guarantee that it will return 1 keys
			expectedKeysCheck: func(s []string) bool {
				if len(s) == 0 {
					return false
				}

				storedValues := make(map[string]bool)
				for _, key := range s {
					if storedValues[key] {
						return false
					}

					if !strings.HasPrefix(key, "pagekey") {
						return false
					}

					storedValues[key] = true
				}

				return true
			},
			expectedCursorCheck: func(cursorMap map[string]uint64) bool {
				if len(cursorMap) == 0 {
					return false
				}

				for _, c := range cursorMap {
					if c == 0 {
						return false
					}
				}

				return true
			},
			continueScanCheck: true,
			expectedErr:       nil,
		},
		{
			name: "count_higher_than_actual_keys",
			setup: func(kv model.KeyValue) {
				ctx := context.Background()
				for i := 0; i < 15; i++ {
					assert.NoError(t, kv.Set(ctx, fmt.Sprintf("cursorkey%d", i), fmt.Sprintf("value%d", i), 0))
				}
			},
			searchStr: "cursorkey*",
			cursor:    nil,
			count:     100,
			expectedKeysCheck: func(s []string) bool {
				return len(s) == 15
			},
			expectedCursorCheck: func(cursorMap map[string]uint64) bool {
				if len(cursorMap) == 0 {
					return false
				}

				for _, c := range cursorMap {
					if c != 0 {
						return false
					}
				}

				return true
			},
			expectedErr: nil,
		},
		{
			name: "mixed_cursors_in_cluster",
			setup: func(kv model.KeyValue) {
				ctx := context.Background()
				for i := 0; i < 15; i++ {
					assert.NoError(t, kv.Set(ctx, fmt.Sprintf("mixedkey%d", i), fmt.Sprintf("value%d", i), 0))
				}
			},
			searchStr: "mixed*",
			cursor:    nil,
			count:     2,
			expectedKeysCheck: func(s []string) bool {
				return len(s) != 0
			},
			expectedCursorCheck: func(cursorMap map[string]uint64) bool {
				// Ensuring some cursors are zero and others are not
				var zeroExists, nonZeroExists bool
				for _, c := range cursorMap {
					if c == 0 {
						zeroExists = true
					} else {
						nonZeroExists = true
					}
				}
				return zeroExists && nonZeroExists
			},
			continueScanCheck: true,
			expectedErr:       nil,
			onlyCluster:       true,
		},
		{
			name:      "test_with_error_condition",
			searchStr: "keys*",
			cursor:    nil,
			count:     10,
			expectedKeysCheck: func(s []string) bool {
				return false
			},
			expectedCursorCheck: func(cursorMap map[string]uint64) bool {
				if len(cursorMap) == 0 {
					return false
				}

				for _, c := range cursorMap {
					if c != 0 {
						return false
					}
				}

				return true
			},
			expectedErr: temperr.ClosedConnection,
		},
	}

	for _, connector := range connectors {
		for _, tc := range tcs {
			t.Run(connector.Type()+"_"+tc.name, func(t *testing.T) {
				ctx := context.Background()
				if errors.Is(tc.expectedErr, temperr.ClosedConnection) {
					assert.NoError(t, connector.Disconnect(ctx))
				} else {
					flusher, err := flusher.NewFlusher(connector)
					assert.Nil(t, err)
					defer func(ctx context.Context) {
						err := flusher.FlushAll(ctx)
						assert.Nil(t, err)
					}(ctx)
				}
				kv, err := NewKeyValue(connector)
				assert.Nil(t, err)

				if tc.setup != nil {
					tc.setup(kv)
				}

				keys, newCursor, continueScan, err := kv.GetKeysWithOpts(ctx, tc.searchStr, tc.cursor, tc.count)
				assert.Equal(t, tc.expectedErr, err)
				assert.Equal(t, tc.continueScanCheck, continueScan)
				if err == nil {
					assert.True(t, tc.expectedKeysCheck(keys))
					if tc.onlyCluster {
						if os.Getenv("REDIS_CLUSTER") == "true" {
							assert.True(t, tc.expectedCursorCheck(newCursor))
						}
						return
					}

					assert.True(t, tc.expectedCursorCheck(newCursor))
				}
			})
		}
	}
}
