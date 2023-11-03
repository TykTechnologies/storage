package temporal

import (
	"context"
	"testing"

	"github.com/go-redis/redis/v8"
)

func TestRedisPing(t *testing.T) {
	rdb := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
		DB:   0, // default db
	})

	pong, err := rdb.Ping(context.Background()).Result()
	if err != nil {
		t.Fatalf("Couldn't connect to Redis: %v", err)
	}

	if pong != "PONG" {
		t.Fatalf("Redis didn't respond with PONG when Pinged. Got: %v", pong)
	}
}
