package redisv8

import (
	"context"

	connectorType "github.com/TykTechnologies/storage/temporal/connector/types"
	"github.com/TykTechnologies/storage/temporal/list/internal/types"
	"github.com/go-redis/redis/v8"
)

type RedisV8List struct {
	connector connectorType.Connector
	client    redis.UniversalClient
}

// NewList returns a new redisv8List instance.
func NewList(conn connectorType.Connector) (*RedisV8List, error) {
	var client redis.UniversalClient
	if conn == nil || conn.As(&client) == false {
		return nil, types.ErrInvalidConnector
	}

	return &RedisV8List{connector: conn, client: client}, nil
}

// Remove the first count occurrences of elements equal to element from the list stored at key.
// The count argument influences the operation in the following ways:
// count > 0: Remove elements equal to element moving from head to tail.
// count < 0: Remove elements equal to element moving from tail to head.
// count = 0: Remove all elements equal to element.
// Equivalent of LRem.
func (r *RedisV8List) Remove(ctx context.Context, key string, count int64, element interface{}) (int64, error) {
	return r.client.LRem(ctx, key, count, element).Result()
}

// Returns the specified elements of the list stored at key.
// The offsets start and stop are zero-based indexes, with 0 being the first element of the list (the head of the list),
// 1 being the next element and so on.
// Equivalent of LRange.
func (r *RedisV8List) Range(ctx context.Context, key string, start, stop int64) ([]string, error) {
	return r.client.LRange(ctx, key, start, stop).Result()
}

// Returns the length of the list stored at key.
// If key does not exist, it is interpreted as an empty list and 0 is returned.
// An error is returned when the value stored at key is not a list.
// Equivalent of LLen.
func (r *RedisV8List) Length(ctx context.Context, key string) (int64, error) {
	return r.client.LLen(ctx, key).Result()
}

// Insert all the specified values at the head of the list stored at key.
// If key does not exist, it is created as empty list before performing the push operations.
// When key holds a value that is not a list, an error is returned.
// Equivalent to LPush.
func (r *RedisV8List) Prepend(ctx context.Context, pipelined bool, key string, values ...[]byte) error {
	if pipelined {
		pipe := r.client.Pipeline()
		defer pipe.Close()

		for _, value := range values {
			pipe.LPush(ctx, key, value)
		}

		_, err := pipe.Exec(ctx)

		return err
	}

	for _, value := range values {
		if err := r.client.LPush(ctx, key, value).Err(); err != nil {
			return err
		}
	}

	return nil
}

// Insert all the specified values at the tail of the list stored at key.
// If key does not exist, it is created as empty list before performing the push operations.
// When key holds a value that is not a list, an error is returned.
// Equivalent to RPush.
func (r *RedisV8List) Append(ctx context.Context, pipelined bool, key string, values ...[]byte) error {
	if pipelined {
		pipe := r.client.Pipeline()
		defer pipe.Close()

		for _, value := range values {
			pipe.RPush(ctx, key, value)
		}

		_, err := pipe.Exec(ctx)

		return err
	}

	for _, value := range values {
		if err := r.client.RPush(ctx, key, value).Err(); err != nil {
			return err
		}
	}

	return nil
}

// Pop removes and returns the first count elements of the list stored at key.
// If stop is -1, all the elements from start to the end of the list are removed and returned.
func (r *RedisV8List) Pop(ctx context.Context, key string, stop int64) ([]string, error) {
	var res *redis.StringSliceCmd

	_, err := r.client.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
		search := stop
		if search > 0 {
			search--
		}
		res = pipe.LRange(ctx, key, 0, search)

		if stop == -1 {
			pipe.Del(ctx, key)
		} else {
			pipe.LTrim(ctx, key, stop, -1)
		}

		return nil
	})
	if err != nil {
		return nil, err
	}

	return res.Result()
}
