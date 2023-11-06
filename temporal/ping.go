package temporal

import (
	"context"
	"fmt"

	"github.com/go-redis/redis/v8"
)

func Ping() error {
	rdb := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
		DB:   0, // default db
	})

	pong, err := rdb.Ping(context.Background()).Result()
	if err != nil {
		return fmt.Errorf("couldn't connect to Redis: %w", err)
	}

	if pong != "PONG" {
		return fmt.Errorf("Redis didn't respond with PONG when Pinged. Got: %v", pong)
	}

	return nil
}
