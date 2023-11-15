package types

import (
	"context"
	"time"

	connectorTypes "github.com/TykTechnologies/storage/temporal/connector/types"
)

type KeyValue interface {
	// Connector adds all the methods from the Connector interface
	connectorTypes.Connector
	// Get retrieves the value for a given key
	Get(ctx context.Context, key string) (value string, err error)
	// Set sets the string value of a key
	Set(ctx context.Context, key, value string, ttl time.Duration) error
	// Delete removes the specified keys
	Delete(ctx context.Context, key string) error
	// Increment atomically increments the integer value of a key by one
	Increment(ctx context.Context, key string) (newValue int64, err error)
	// Decrement atomically decrements the integer value of a key by one
	Decrement(ctx context.Context, key string) (newValue int64, err error)
	// Exists checks if a key exists
	Exists(ctx context.Context, key string) (exists bool, err error)
	// Expire sets a timeout on key. After the timeout has expired, the key will automatically be deleted
	Expire(ctx context.Context, key string, ttl time.Duration) error
	// TTL returns the remaining time to live of a key that has a timeout
	TTL(ctx context.Context, key string) (ttl int64, err error)
	// DeleteKeys deletes all keys that match the given pattern
	DeleteKeys(ctx context.Context, keys []string) (numberOfDeletedKeys int64, err error)
	// DeleteScanMatch deletes all keys that match the given pattern
	DeleteScanMatch(ctx context.Context, pattern string) (numberOfDeletedKeys int64, err error)
	// Keys returns all keys that match the given pattern
	Keys(ctx context.Context, pattern string) (keys []string, err error)
	// GetMulti returns the values of all specified keys
	GetMulti(ctx context.Context, keys []string) (values []interface{}, err error)
	// GetKeysAndValuesWithFilter returns all keys and values that match the given pattern
	GetKeysAndValuesWithFilter(ctx context.Context, pattern string) (keysAndValues map[string]interface{}, err error)
}
