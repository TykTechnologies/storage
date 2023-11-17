package model

import (
	"context"
	"time"
)

type Connector interface {
	// Disconnect disconnects from the backend
	Disconnect(context.Context) error

	// Ping executes a ping to the backend
	Ping(context.Context) error

	// Type returns the  connector type
	Type() string

	// As converts i to driver-specific types.
	// Same concept as https://gocloud.dev/concepts/as/ but for connectors.
	As(i interface{}) bool
}

type List interface {
	// Remove the first count occurrences of elements equal to element from the list stored at key.
	Remove(ctx context.Context, key string, count int64, element interface{}) (int64, error)

	// Returns the specified elements of the list stored at key.
	// The offsets start and stop are zero-based indexes.
	Range(ctx context.Context, key string, start, stop int64) ([]string, error)

	// Returns the length of the list stored at key.
	Length(ctx context.Context, key string) (int64, error)

	// Insert all the specified values at the head of the list stored at key.
	// If key does not exist, it is created.
	// pipelined: If true, the operation is pipelined and executed in a single roundtrip.
	Prepend(ctx context.Context, pipelined bool, key string, values ...[]byte) error

	// Insert all the specified values at the tail of the list stored at key.
	// If key does not exist, it is created.
	// pipelined: If true, the operation is pipelined and executed in a single roundtrip.
	Append(ctx context.Context, pipelined bool, key string, values ...[]byte) error

	// Pop removes and returns the first count elements of the list stored at key.
	// If stop is -1, all the elements from start to the end of the list are removed and returned.
	Pop(ctx context.Context, key string, stop int64) ([]string, error)
}

type KeyValue interface {
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

type Flusher interface {
	// FlushAll deletes all keys the database
	FlushAll(ctx context.Context) error
}
