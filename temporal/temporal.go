package temporal

import (
	"errors"

	"github.com/TykTechnologies/storage/temporal/internal/driver/redis8"
	"github.com/TykTechnologies/storage/temporal/internal/types"
)

const (
	Redis8 string = "redis-8"
)

func NewKeyValue(opts *types.ClientOpts) (types.KeyValue, error) {
	switch opts.Type {
	case Redis8:
		return redis8.NewRedis8(opts)
	default:
		return nil, errors.New("invalid driver")
	}
}
