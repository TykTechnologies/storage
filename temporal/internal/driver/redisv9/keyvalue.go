package redisv9

import (
	"context"
	"errors"
	"strings"
	"sync"
	"time"

	"github.com/TykTechnologies/storage/temporal/temperr"
	"github.com/redis/go-redis/v9"
)

// Get retrieves the value for a given key from Redis
func (r *RedisV9) Get(ctx context.Context, key string) (string, error) {
	if key == "" {
		return "", temperr.KeyEmpty
	}

	result, err := r.client.Get(ctx, key).Result()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return "", temperr.KeyNotFound
		}

		return "", err
	}

	return result, nil
}

// Set sets the string value of a key
func (r *RedisV9) Set(ctx context.Context, key, value string, expiration time.Duration) error {
	if key == "" {
		return temperr.KeyEmpty
	}

	return r.client.Set(ctx, key, value, expiration).Err()
}

// Delete removes the specified keys
func (r *RedisV9) Delete(ctx context.Context, key string) error {
	if key == "" {
		return temperr.KeyEmpty
	}

	_, err := r.client.Del(ctx, key).Result()

	return err
}

// Increment atomically increments the integer value of a key by one
func (r *RedisV9) Increment(ctx context.Context, key string) (int64, error) {
	if key == "" {
		return 0, temperr.KeyEmpty
	}

	res, err := r.client.Incr(ctx, key).Result()
	if err != nil && strings.EqualFold(err.Error(), "ERR value is not an integer or out of range") {
		return 0, temperr.KeyMisstype
	}

	return res, err
}

// Decrement atomically decrements the integer value of a key by one
func (r *RedisV9) Decrement(ctx context.Context, key string) (int64, error) {
	if key == "" {
		return 0, temperr.KeyEmpty
	}

	res, err := r.client.Decr(ctx, key).Result()
	if err != nil && strings.EqualFold(err.Error(), "ERR value is not an integer or out of range") {
		return 0, temperr.KeyMisstype
	}

	return res, err
}

// Exists checks if a key exists
func (r *RedisV9) Exists(ctx context.Context, key string) (bool, error) {
	if key == "" {
		return false, temperr.KeyEmpty
	}

	result, err := r.client.Exists(ctx, key).Result()

	return result > 0, err
}

// Expire sets a timeout on key. After the timeout has expired, the key will automatically be deleted
func (r *RedisV9) Expire(ctx context.Context, key string, expiration time.Duration) error {
	if key == "" {
		return temperr.KeyEmpty
	}

	return r.client.Expire(ctx, key, expiration).Err()
}

// TTL returns the remaining time to live of a key that has a timeout
func (r *RedisV9) TTL(ctx context.Context, key string) (int64, error) {
	if key == "" {
		return 0, temperr.KeyEmpty
	}

	duration, err := r.client.TTL(ctx, key).Result()
	if err != nil {
		return 0, err
	}

	return int64(duration.Seconds()), nil
}

// DeleteKeys removes the specified keys. A key is ignored if it does not exist
func (r *RedisV9) DeleteKeys(ctx context.Context, keys []string) (int64, error) {
	if len(keys) == 0 {
		return 0, temperr.KeyEmpty
	}

	switch v := r.client.(type) {
	case *redis.ClusterClient:
		return r.deleteKeysCluster(ctx, v, keys)
	case *redis.Client:
		return v.Del(ctx, keys...).Result()
	default:
		return 0, temperr.InvalidRedisClient
	}
}

// deleteKeysCluster removes the specified keys on a cluster
func (r *RedisV9) deleteKeysCluster(ctx context.Context, cluster *redis.ClusterClient, keys []string) (int64, error) {
	var totalDeleted int64

	for _, key := range keys {
		delCmd := redis.NewIntCmd(ctx, "DEL", key)

		// Process the command, which sends it to the appropriate node
		if err := cluster.Process(ctx, delCmd); err != nil {
			return totalDeleted, err
		}

		// Accumulate the count of deleted keys
		deleted, err := delCmd.Result()
		if err != nil {
			return totalDeleted, err
		}

		totalDeleted += deleted
	}

	return totalDeleted, nil
}

// DeleteScanMatch deletes all keys matching the given pattern
func (r *RedisV9) DeleteScanMatch(ctx context.Context, pattern string) (int64, error) {
	var totalDeleted int64
	var mutex sync.Mutex
	var firstError error

	switch client := r.client.(type) {
	case *redis.ClusterClient:
		err := client.ForEachMaster(ctx, func(ctx context.Context, client *redis.Client) error {
			deleted, err := r.deleteScanMatchSingleNode(ctx, client, pattern)
			if err != nil {
				if errors.Is(err, redis.ErrClosed) {
					err = temperr.ClosedConnection
				}

				if firstError == nil {
					firstError = err
				}
				return nil // Continue with other nodes
			}

			mutex.Lock()
			totalDeleted += deleted
			mutex.Unlock()

			return nil
		})

		if firstError != nil {
			return totalDeleted, firstError
		}
		if err != nil {
			return totalDeleted, err
		}

	case *redis.Client:
		var err error
		totalDeleted, err = r.deleteScanMatchSingleNode(ctx, client, pattern)

		if err != nil {
			return totalDeleted, err
		}

	default:
		return totalDeleted, temperr.InvalidRedisClient
	}

	return totalDeleted, nil
}

// deleteScanMatchSingleNode deletes all keys matching the given pattern on a single node
func (r *RedisV9) deleteScanMatchSingleNode(ctx context.Context, client redis.Cmdable, pattern string) (int64, error) {
	var deleted, cursor uint64
	var err error

	var keys []string
	keys, _, err = client.Scan(ctx, cursor, pattern, 0).Result()

	if err != nil {
		return int64(deleted), err
	}

	if len(keys) > 0 {
		del, err := client.Del(ctx, keys...).Result()
		if err != nil {
			return int64(deleted), err
		}

		deleted += uint64(del)
	}

	return int64(deleted), nil
}

// Keys returns all keys matching the given pattern
func (r *RedisV9) Keys(ctx context.Context, pattern string) ([]string, error) {
	var sessions []string
	var mutex sync.Mutex
	var firstError error

	switch client := r.client.(type) {
	case *redis.ClusterClient:
		err := client.ForEachMaster(ctx, func(ctx context.Context, client *redis.Client) error {
			keys, _, err := fetchKeys(ctx, client, pattern, 0, 0)
			if err != nil {
				if errors.Is(err, redis.ErrClosed) {
					err = temperr.ClosedConnection
				}

				if firstError == nil {
					firstError = err
				}
				return nil // continue with other nodes
			}

			mutex.Lock()
			sessions = append(sessions, keys...)
			mutex.Unlock()

			return nil
		})

		if firstError != nil {
			return nil, firstError
		}
		if err != nil {
			return nil, err
		}

	case *redis.Client:
		keys, _, err := fetchKeys(ctx, client, pattern, 0, 0)
		if err != nil {
			return nil, err
		}

		sessions = keys

	default:
		return nil, temperr.InvalidRedisClient
	}

	return sessions, nil
}

// GetMulti returns the values of all specified keys
func (r *RedisV9) GetMulti(ctx context.Context, keys []string) ([]interface{}, error) {
	switch client := r.client.(type) {
	case *redis.ClusterClient:
		return r.getMultiCluster(ctx, client, keys)
	case *redis.Client:
		return r.getMultiStandalone(ctx, client, keys)
	default:
		return nil, temperr.InvalidRedisClient
	}
}

func (r *RedisV9) getMultiCluster(ctx context.Context,
	client *redis.ClusterClient,
	keys []string,
) ([]interface{}, error) {
	values := make([]interface{}, len(keys))

	for i, key := range keys {
		value, err := r.getValueFromCluster(ctx, client, key)
		if err != nil {
			return nil, err
		}

		values[i] = value
	}

	return values, nil
}

func (r *RedisV9) getValueFromCluster(ctx context.Context,
	client *redis.ClusterClient,
	key string,
) (interface{}, error) {
	cmd := client.Get(ctx, key)
	if err := cmd.Err(); err != nil {
		if errors.Is(err, redis.Nil) {
			return nil, nil
		}

		return nil, err
	}

	val := cmd.Val()
	if val == "" {
		return nil, nil
	}

	return val, nil
}

func (r *RedisV9) getMultiStandalone(ctx context.Context, client *redis.Client, keys []string) ([]interface{}, error) {
	cmd := client.MGet(ctx, keys...)
	if cmd.Err() != nil {
		return nil, cmd.Err()
	}
	return cmd.Val(), nil
}

// GetKeysAndValuesWithFilter returns all keys and their values for a given pattern
func (r *RedisV9) GetKeysAndValuesWithFilter(ctx context.Context,
	pattern string,
) (map[string]interface{}, error) {
	keys, err := r.Keys(ctx, pattern)
	if err != nil {
		return nil, err
	}

	result := make(map[string]interface{})

	if len(keys) == 0 {
		return result, nil
	}

	values, err := r.GetMulti(ctx, keys)
	if err != nil {
		return nil, err
	}

	for i, key := range keys {
		result[key] = values[i]
	}

	return result, nil
}

// GetKeysWithOpts retrieves keys with options like filter, cursor, and count
func (r *RedisV9) GetKeysWithOpts(ctx context.Context,
	searchStr string,
	cursor uint64,
	count int,
) ([]string, error) {
	var keys []string
	var mutex sync.Mutex
	var firstError error

	switch client := r.client.(type) {
	case *redis.ClusterClient:
		err := client.ForEachMaster(ctx, func(ctx context.Context, client *redis.Client) error {
			localKeys, _, err := fetchKeys(ctx, client, searchStr, cursor, int64(count))
			if err != nil {
				if errors.Is(err, redis.ErrClosed) {
					err = temperr.ClosedConnection
				}

				if firstError == nil {
					firstError = err
				}
				return nil // Continue to next node
			}

			mutex.Lock()
			keys = append(keys, localKeys...)
			mutex.Unlock()

			return nil
		})

		if firstError != nil {
			return keys, firstError
		}

		if err != nil {
			return keys, err
		}

	case *redis.Client:
		localKeys, _, err := fetchKeys(ctx, client, searchStr, cursor, int64(count))
		if err != nil {
			if errors.Is(err, redis.ErrClosed) {
				return localKeys, temperr.ClosedConnection
			}

			return localKeys, err
		}

		keys = localKeys

	default:
		return nil, temperr.InvalidRedisClient
	}

	return keys, nil
}

func fetchKeys(ctx context.Context,
	client redis.UniversalClient,
	pattern string,
	cursor uint64,
	count int64,
) ([]string, uint64, error) {
	var keys []string

	for {
		var iterKeys []string
		var err error

		iterKeys, cursor, err = client.Scan(ctx, cursor, pattern, count).Result()
		if err != nil {
			return nil, 0, err
		}

		keys = append(keys, iterKeys...)

		if cursor == 0 {
			break
		}
	}

	return keys, cursor, nil
}
