package temporal

import (
	"context"
	"fmt"
	"os"

	"github.com/go-redis/redis/v8"
)

func Ping() error {

	redisAddr := os.Getenv("REDIS_ADDR")
	if redisAddr == "" {
		return fmt.Errorf("REDIS_ADDR environment variable not set")
	}

	rdb := redis.NewClient(&redis.Options{
		Addr: redisAddr,
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
