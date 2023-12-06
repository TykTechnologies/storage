package redisv8

import (
	"context"
	"strconv"
	"time"

	"github.com/TykTechnologies/storage/temporal/temperr"
	"github.com/go-redis/redis/v8"
)

// SetRollingWindow sets a rolling window of values in a Redis sorted set.
// It returns a slice of strings (the values in the rolling window) and an error if any occurs.
func (r *RedisV8) SetRollingWindow(ctx context.Context, keyName string,
	per int64, value_override string, pipeline bool,
) ([]string, error) {
	now := time.Now()
	onePeriodAgo := now.Add(time.Duration(-1*per) * time.Second)
	var zrange *redis.StringSliceCmd
	var err error

	if keyName == "" {
		return []string{}, temperr.KeyEmpty
	}

	if per <= 0 {
		return []string{}, temperr.InvalidPeriod
	}

	pipeFn := func(pipe redis.Pipeliner) error {
		pipe.ZRemRangeByScore(ctx, keyName, "-inf", strconv.Itoa(int(onePeriodAgo.UnixNano())))
		zrange = pipe.ZRange(ctx, keyName, 0, -1)

		element := redis.Z{
			Score: float64(now.UnixNano()),
		}

		if value_override != "-1" {
			element.Member = value_override
		} else {
			element.Member = strconv.Itoa(int(now.UnixNano()))
		}

		pipe.ZAdd(ctx, keyName, &element)
		pipe.Expire(ctx, keyName, time.Duration(per)*time.Second)

		return nil
	}

	if pipeline {
		_, err = r.client.Pipelined(ctx, pipeFn)
	} else {
		_, err = r.client.TxPipelined(ctx, pipeFn)
	}

	if err != nil {
		return []string{}, err
	}

	return zrange.Result()
}

// GetRollingWindow retrieves a rolling window of values from a Redis sorted set.
// It returns a slice of strings (the values in the rolling window) and an error if any occurs.
func (r *RedisV8) GetRollingWindow(ctx context.Context, keyName string, per int64, pipeline bool) ([]string, error) {
	now := time.Now()
	onePeriodAgo := now.Add(time.Duration(-1*per) * time.Second)

	var zrange *redis.StringSliceCmd
	var err error

	pipeFn := func(pipe redis.Pipeliner) error {
		pipe.ZRemRangeByScore(ctx, keyName, "-inf", strconv.FormatInt(onePeriodAgo.UnixNano(), 10))
		zrange = pipe.ZRange(ctx, keyName, 0, -1)

		return nil
	}

	if pipeline {
		_, err = r.client.Pipelined(ctx, pipeFn)
	} else {
		_, err = r.client.TxPipelined(ctx, pipeFn)
	}

	if err != nil {
		return nil, err
	}

	values, err := zrange.Result()
	if err != nil {
		return nil, err
	}

	if values == nil {
		return []string{}, nil
	}

	return values, nil
}
