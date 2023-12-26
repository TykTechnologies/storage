package redisv8

import (
	"context"
	"strconv"
	"time"

	"github.com/TykTechnologies/storage/temporal/temperr"
	"github.com/go-redis/redis/v8"
)

// SetRollingWindow updates a sorted set in Redis to represent a rolling time window of values.
func (r *RedisV8) SetRollingWindow(ctx context.Context, now time.Time, keyName string, per int64, valueOverride string, pipeline bool) ([]string, error) {
	if keyName == "" {
		return []string{}, temperr.KeyEmpty
	}
	if per <= 0 {
		return []string{}, temperr.InvalidPeriod
	}

	onePeriodAgo := now.Add(time.Duration(-1*per) * time.Second)
	expire := time.Duration(per) * time.Second

	memberValue := valueOverride
	if valueOverride == "-1" {
		memberValue = strconv.Itoa(int(now.UnixNano()))
	}
	element := redis.Z{
		Score:  float64(now.UnixNano()),
		Member: memberValue,
	}

	var zrange *redis.StringSliceCmd
	var err error

	exec := r.client.TxPipelined
	if pipeline {
		exec = r.client.Pipelined
	}

	pipeFn := func(pipe redis.Pipeliner) error {
		// removing elements outside the rolling window.
		pipe.ZRemRangeByScore(ctx, keyName, "-inf", strconv.Itoa(int(onePeriodAgo.UnixNano())))
		// getting the current range of values within the window.
		zrange = pipe.ZRange(ctx, keyName, 0, -1)
		// adding the new element and set the expiration time.
		pipe.ZAdd(ctx, keyName, &element)
		pipe.Expire(ctx, keyName, expire)
		return nil
	}

	_, err = exec(ctx, pipeFn)
	if err != nil {
		return nil, err
	}

	return zrange.Result()
}

// GetRollingWindow removes a part of a sorted set in Redis and extracts a timed window of values.
func (r *RedisV8) GetRollingWindow(ctx context.Context, now time.Time, keyName string, per int64, pipeline bool) ([]string, error) {
	if keyName == "" {
		return []string{}, temperr.KeyEmpty
	}
	if per <= 0 {
		return []string{}, temperr.InvalidPeriod
	}

	onePeriodAgo := now.Add(time.Duration(-1*per) * time.Second)
	period := strconv.FormatInt(onePeriodAgo.UnixNano(), 10)

	var zrange *redis.StringSliceCmd
	var err error

	exec := r.client.TxPipelined
	if pipeline {
		exec = r.client.Pipelined
	}

	pipeFn := func(pipe redis.Pipeliner) error {
		// removing old elements outside the rolling window
		pipe.ZRemRangeByScore(ctx, keyName, "-inf", period)
		// retrieving the current range of values
		zrange = pipe.ZRange(ctx, keyName, 0, -1)
		return nil
	}

	_, err = exec(ctx, pipeFn)
	if err != nil {
		return nil, err
	}

	return zrange.Result()
}
