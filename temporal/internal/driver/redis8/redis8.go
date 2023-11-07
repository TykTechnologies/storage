package redis8

import "github.com/go-redis/redis/v8"

type Redis8 struct {
	Client *redis.Client
}

func NewRedis8(addr, password string, db int) *Redis8 {
	rdb := redis.NewClient(&redis.Options{
		Addr:     addr,
		Password: password,
		DB:       db,
	})

	return &Redis8{Client: rdb}
}
