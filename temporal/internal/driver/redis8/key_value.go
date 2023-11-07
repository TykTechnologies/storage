package redis8

import (
	"context"
	"errors"
	"time"

	"github.com/go-redis/redis/v8"
)

// Get retrieves the value for a given key from Redis.
func (r *Redis8) Get(ctx context.Context, key string) (string, error) {
	result, err := r.Client.Get(ctx, key).Result()
	if err != nil {
		if err == redis.Nil {
			return "", nil // or fmt.Errorf("key not found")?
		}
		return "", err
	}

	return result, nil
}

// Set sets the string value of a key.
func (r *Redis8) Set(ctx context.Context, key string, value string, expiration time.Duration) error {
	// TBD: should we return an error if the key is empty?
	if key == "" {
		return errors.New("key cannot be empty")
	}
	return r.Client.Set(ctx, key, value, expiration).Err()
}

// Delete removes the specified keys.
func (r *Redis8) Delete(ctx context.Context, key string) error {
	_, err := r.Client.Del(ctx, key).Result()
	return err
}

// Increment atomically increments the integer value of a key by one.
func (r *Redis8) Increment(ctx context.Context, key string) (int64, error) {
	return r.Client.Incr(ctx, key).Result()
}

// Decrement atomically decrements the integer value of a key by one.
func (r *Redis8) Decrement(ctx context.Context, key string) (int64, error) {
	return r.Client.Decr(ctx, key).Result()
}

// Exists checks if a key exists.
func (r *Redis8) Exists(ctx context.Context, key string) (bool, error) {
	result, err := r.Client.Exists(ctx, key).Result()
	return result > 0, err
}
