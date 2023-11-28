package redisv8

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

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

	return r.client.Del(ctx, keys...).Result()
}

// DeleteScanMatch uses the SCAN command to find all keys matching the given pattern and deletes them
func (r *RedisV8) DeleteScanMatch(ctx context.Context, pattern string) (int64, error) {
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

func (r *RedisV8) fetchKeys(ctx context.Context, pattern string) ([]string, error) {
	values := make([]string, 0)
	iter := r.client.Scan(ctx, 0, pattern, 0).Iterator()

	for iter.Next(ctx) {
		values = append(values, iter.Val())
		if err := iter.Err(); err != nil {
			return nil, err
		}
	}

	return values, nil
}

// Keys returns all keys matching the given pattern
func (r *RedisV8) Keys(ctx context.Context, pattern string) ([]string, error) {
	sessions := make([]string, 0)
	var err error
	switch client := r.client.(type) {
	case *redis.ClusterClient:
		ch := make(chan []string)
		go func() {
			err = client.ForEachMaster(ctx, func(context context.Context, client *redis.Client) error {
				result, err := r.fetchKeys(ctx, pattern)
				if err != nil {
					return err
				}

				ch <- result
				return nil
			})
			close(ch)
		}()

		for v := range ch {
			sessions = append(sessions, v...)
		}

	case *redis.Client:
		sessions, err = r.fetchKeys(ctx, pattern)
	}

	if err != nil {
		return nil, err
	}

	return sessions, nil
}

// GetMulti returns the values of all specified keys
func (r *RedisV8) GetMulti(ctx context.Context, keys []string) ([]interface{}, error) {
	switch client := r.client.(type) {
	case *redis.ClusterClient:
		getCmds := make([]*redis.StringCmd, 0)
		pipe := client.Pipeline()
		for _, key := range keys {
			getCmds = append(getCmds, pipe.Get(ctx, key))
		}

		_, err := pipe.Exec(ctx)
		if err != nil && err != redis.Nil {
			return nil, err
		}

		values := make([]interface{}, len(getCmds))
		for i, cmd := range getCmds {
			if cmd.Err() != nil && cmd.Err() != redis.Nil {
				values[i] = nil
				continue
			}
			values[i] = cmd.Val()
		}
		return values, nil

	case *redis.Client:
		cmd := client.MGet(ctx, keys...)
		if cmd.Err() != nil {
			return nil, cmd.Err()
		}
		return cmd.Val(), nil

	default:
		return nil, fmt.Errorf("unsupported Redis client type")
	}
}

// GetKeysAndValuesWithFilter returns all keys and their values for a given pattern
func (r *RedisV8) GetKeysAndValuesWithFilter(ctx context.Context,
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
func (r *RedisV8) GetKeysWithOpts(ctx context.Context, searchStr string, cursor uint64, count int) (model.KeysCursorPair, error) {
	result := model.KeysCursorPair{}

	fnFetchKeys := func(client *redis.Client, cursor uint64, count int) (model.KeysCursorPair, error) {
		result := model.KeysCursorPair{}

		result.Keys = make([]string, 0)

		iter := client.Scan(ctx, cursor, searchStr, int64(count))

		if err := iter.Err(); err != nil {
			return result, err
		}

		result.Keys, result.Cursor = iter.Val()

		return result, nil
	}

	var err error

	switch v := r.client.(type) {
	case *redis.ClusterClient:
		ch := make(chan model.KeysCursorPair)

		go func() {
			err = v.ForEachMaster(ctx, func(context context.Context, client *redis.Client) error {
				select {
				case <-ctx.Done():
					return errors.New("context cancelled while looking into redis")
				default:
				}

				// TODO check if each master returns different cursors
				result, err := fnFetchKeys(client, cursor, count)
				if err != nil {
					return err
				}

				ch <- result
				return nil
			})
			close(ch)
		}()

		for v := range ch {
			result.Keys = append(result.Keys, v.Keys...)
		}

	case *redis.Client:
		result, err = fnFetchKeys(v, cursor, count)
	}

	return result, err
}
