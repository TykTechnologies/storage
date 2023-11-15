package redisv8

import (
	"context"
	"errors"
	"time"

	"github.com/TykTechnologies/storage/temporal/keyvalue/types"
	"github.com/go-redis/redis/v8"
)

// Get retrieves the value for a given key from Redis
func (r *KeyValueRedisV8) Get(ctx context.Context, key string) (string, error) {
	result, err := r.client.Get(ctx, key).Result()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return "", types.ErrKeyNotFound
		}

		return "", err
	}

	return result, nil
}

// Set sets the string value of a key
func (r *KeyValueRedisV8) Set(ctx context.Context, key, value string, expiration time.Duration) error {
	if key == "" {
		return types.ErrKeyNotEmpty
	}

	return r.client.Set(ctx, key, value, expiration).Err()
}

// Delete removes the specified keys
func (r *KeyValueRedisV8) Delete(ctx context.Context, key string) error {
	_, err := r.client.Del(ctx, key).Result()
	return err
}

// Increment atomically increments the integer value of a key by one
func (r *KeyValueRedisV8) Increment(ctx context.Context, key string) (int64, error) {
	return r.client.Incr(ctx, key).Result()
}

// Decrement atomically decrements the integer value of a key by one
func (r *KeyValueRedisV8) Decrement(ctx context.Context, key string) (int64, error) {
	return r.client.Decr(ctx, key).Result()
}

// Exists checks if a key exists
func (r *KeyValueRedisV8) Exists(ctx context.Context, key string) (bool, error) {
	result, err := r.client.Exists(ctx, key).Result()
	return result > 0, err
}

// Expire sets a timeout on key. After the timeout has expired, the key will automatically be deleted
func (r *KeyValueRedisV8) Expire(ctx context.Context, key string, expiration time.Duration) error {
	return r.client.Expire(ctx, key, expiration).Err()
}

// TTL returns the remaining time to live of a key that has a timeout
func (r *KeyValueRedisV8) TTL(ctx context.Context, key string) (int64, error) {
	duration, err := r.client.TTL(ctx, key).Result()
	if err != nil {
		return 0, err
	}

	return int64(duration.Seconds()), nil
}

// DeleteKeys removes the specified keys. A key is ignored if it does not exist
func (r *KeyValueRedisV8) DeleteKeys(ctx context.Context, keys []string) (int64, error) {
	return r.client.Del(ctx, keys...).Result()
}

// DeleteScanMatch uses the SCAN command to find all keys matching the given pattern and deletes them
func (r *KeyValueRedisV8) DeleteScanMatch(ctx context.Context, pattern string) (int64, error) {
	var deleted int64
	var cursor uint64
	var err error

	for {
		var keys []string

		keys, cursor, err = r.client.Scan(ctx, cursor, pattern, 0).Result()
		if err != nil {
			return deleted, err
		}

		if len(keys) > 0 {
			var del int64

			del, err = r.client.Del(ctx, keys...).Result()
			if err != nil {
				return deleted, err
			}

			deleted += del
		}

		if cursor == 0 {
			break
		}
	}

	return deleted, nil
}

// Keys returns all keys matching the given pattern
func (r *KeyValueRedisV8) Keys(ctx context.Context, pattern string) ([]string, error) {
	return r.client.Keys(ctx, pattern).Result()
}

// GetMulti returns the values of all specified keys
func (r *KeyValueRedisV8) GetMulti(ctx context.Context, keys []string) ([]interface{}, error) {
	cmd := r.client.MGet(ctx, keys...)
	if cmd.Err() != nil {
		return nil, cmd.Err()
	}

	return cmd.Val(), nil
}

// GetKeysAndValuesWithFilter returns all keys and their values for a given pattern
func (r *KeyValueRedisV8) GetKeysAndValuesWithFilter(ctx context.Context,
	pattern string,
) (map[string]interface{}, error) {
	keys, err := r.Keys(ctx, pattern)
	if err != nil {
		return nil, err
	}

	values, err := r.GetMulti(ctx, keys)
	if err != nil {
		return nil, err
	}

	result := make(map[string]interface{})
	for i, key := range keys {
		result[key] = values[i]
	}

	return result, nil
}
