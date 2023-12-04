package redisv8

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/TykTechnologies/storage/temporal/model"
	"github.com/TykTechnologies/storage/temporal/temperr"
	"github.com/go-redis/redis/v8"
)

// Get retrieves the value for a given key from Redis
func (r *RedisV8) Get(ctx context.Context, key string) (string, error) {
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
func (r *RedisV8) Set(ctx context.Context, key, value string, expiration time.Duration) error {
	if key == "" {
		return temperr.KeyEmpty
	}

	return r.client.Set(ctx, key, value, expiration).Err()
}

// Delete removes the specified keys
func (r *RedisV8) Delete(ctx context.Context, key string) error {
	if key == "" {
		return temperr.KeyEmpty
	}

	_, err := r.client.Del(ctx, key).Result()

	return err
}

// Increment atomically increments the integer value of a key by one
func (r *RedisV8) Increment(ctx context.Context, key string) (int64, error) {
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
func (r *RedisV8) Decrement(ctx context.Context, key string) (int64, error) {
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
func (r *RedisV8) Exists(ctx context.Context, key string) (bool, error) {
	if key == "" {
		return false, temperr.KeyEmpty
	}

	result, err := r.client.Exists(ctx, key).Result()

	return result > 0, err
}

// Expire sets a timeout on key. After the timeout has expired, the key will automatically be deleted
func (r *RedisV8) Expire(ctx context.Context, key string, expiration time.Duration) error {
	if key == "" {
		return temperr.KeyEmpty
	}

	return r.client.Expire(ctx, key, expiration).Err()
}

// TTL returns the remaining time to live of a key that has a timeout
func (r *RedisV8) TTL(ctx context.Context, key string) (int64, error) {
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
func (r *RedisV8) DeleteKeys(ctx context.Context, keys []string) (int64, error) {
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
func (r *RedisV8) deleteKeysCluster(ctx context.Context, cluster *redis.ClusterClient, keys []string) (int64, error) {
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
func (r *RedisV8) DeleteScanMatch(ctx context.Context, pattern string) (int64, error) {
	var totalDeleted int64

	switch client := r.client.(type) {
	case *redis.ClusterClient:
		err := client.ForEachMaster(ctx, func(ctx context.Context, client *redis.Client) error {
			deleted, err := r.deleteScanMatchSingleNode(ctx, client, pattern)
			if err != nil {
				return err
			}

			totalDeleted += deleted
			return nil
		})
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
func (r *RedisV8) deleteScanMatchSingleNode(ctx context.Context, client redis.Cmdable, pattern string) (int64, error) {
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
func (r *RedisV8) Keys(ctx context.Context, pattern string) ([]string, error) {
	var sessions []string

	switch client := r.client.(type) {
	case *redis.ClusterClient:
		err := client.ForEachMaster(ctx, func(ctx context.Context, client *redis.Client) error {
			result, err := fetchKeys(ctx, client, pattern, 0, 0)
			if err != nil {
				return err
			}

			sessions = append(sessions, result.Keys...)
			return nil
		})
		if err != nil {
			return nil, err
		}

	case *redis.Client:
		result, err := fetchKeys(ctx, client, pattern, 0, 0)
		if err != nil {
			return nil, err
		}

		sessions = result.Keys

	default:
		return nil, temperr.InvalidRedisClient
	}

	return sessions, nil
}

// GetMulti returns the values of all specified keys
func (r *RedisV8) GetMulti(ctx context.Context, keys []string) ([]string, error) {
	result := make([]string, 0)

	switch client := r.client.(type) {
	case *redis.ClusterClient:

		getCmds := make([]*redis.StringCmd, 0)
		pipe := client.Pipeline()
		for _, key := range keys {
			getCmds = append(getCmds, pipe.Get(r.client.Context(), key))
		}
		_, err := pipe.Exec(r.client.Context())
		if err != nil && err != redis.Nil {
			return nil, temperr.KeyNotFound
		}
		for _, cmd := range getCmds {
			result = append(result, cmd.Val())
		}

		return result, nil

	case *redis.Client:
		values, err := r.client.MGet(r.client.Context(), keys...).Result()
		if err != nil {
			return nil, temperr.KeyNotFound
		}
		for _, val := range values {
			strVal := fmt.Sprint(val)
			if strVal == "<nil>" {
				strVal = ""
			}
			result = append(result, strVal)
		}
		return result, nil
	default:
		return nil, temperr.InvalidRedisClient
	}
}

// GetKeysAndValuesWithFilter returns all keys and their values for a given pattern
func (r *RedisV8) GetKeysAndValuesWithFilter(ctx context.Context,
	pattern string,
) (map[string]string, error) {
	keys, err := r.Keys(ctx, pattern)
	if err != nil {
		return nil, err
	}

	result := make(map[string]string)

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
func (r *RedisV8) GetKeysWithOpts(ctx context.Context,
	searchStr string,
	cursor uint64,
	count int,
) (model.KeysCursorPair, error) {
	var result model.KeysCursorPair

	switch client := r.client.(type) {
	case *redis.ClusterClient:
		err := client.ForEachMaster(ctx, func(ctx context.Context, client *redis.Client) error {
			localResult, err := fetchKeys(ctx, client, searchStr, cursor, int64(count))
			if err != nil {
				return err
			}

			result.Keys = append(result.Keys, localResult.Keys...)
			result.Cursor = localResult.Cursor
			return nil
		})
		if err != nil {
			if errors.Is(err, redis.ErrClosed) {
				return result, temperr.ClosedConnection
			}

			return result, err
		}

	case *redis.Client:
		var err error
		result, err = fetchKeys(ctx, client, searchStr, cursor, int64(count))

		if err != nil {
			if errors.Is(err, redis.ErrClosed) {
				return result, temperr.ClosedConnection
			}

			return result, err
		}

	default:
		return result, temperr.InvalidRedisClient
	}

	return result, nil
}

// fetchKeys retrieves keys with options like filter, cursor, and count
func fetchKeys(ctx context.Context,
	client redis.UniversalClient,
	pattern string,
	cursor uint64,
	count int64,
) (model.KeysCursorPair, error) {
	result := model.KeysCursorPair{}

	result.Keys = make([]string, 0)

	iter := client.Scan(ctx, cursor, pattern, int64(count))

	if err := iter.Err(); err != nil {
		return result, err
	}

	result.Keys, result.Cursor = iter.Val()

	return result, nil
}
