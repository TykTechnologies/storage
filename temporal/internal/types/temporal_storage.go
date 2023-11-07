package types

import (
	"context"
	"time"
)

type KeyValue interface {
	// connection handling
	//   Handler

	// basic usage
	Get(context.Context, string) (string, error)
	Set(context.Context, string, string, time.Duration) error
	Delete(context.Context, string) error
	Increment(context.Context, string) (int64, error)
	Decrement(context.Context, string) (int64, error)
	Exists(context.Context, string) (bool, error)

	// expiration
	Expire(context.Context, string, time.Duration) error
	TTL(context.Context, string) (int64, error)

	// multi keys ops
	DeleteKeys(ctx context.Context, keys []string) (int64, error)
	DeleteScanMatch(context.Context, string) (int64, error)

	Keys(context.Context, string) ([]string, error)
	//MGet
	GetMulti(context.Context, []string) ([]interface{}, error)
	// this is a combination of Keys()+MGet() - I'm wondering if there's a more idiomatic solution
	// TODO naming
	GetKeysAndValuesWithFilter(context.Context, string) (map[string]interface{}, error)
}
