package redis8

import (
	"github.com/TykTechnologies/storage/temporal/internal/types"
	"github.com/go-redis/redis/v8"
)

type Redis8 struct {
	client *redis.Client
}

func NewRedis8(opts *types.ClientOpts) (*Redis8, error) {
	rdb := redis.NewClient(&redis.Options{
		Addr:     opts.Addr,
		Password: opts.Password,
		DB:       opts.DB,
	})

	return &Redis8{client: rdb}, nil
}
