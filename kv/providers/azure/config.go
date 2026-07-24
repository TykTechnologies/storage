package azure

import (
	"encoding/json"

	"github.com/TykTechnologies/storage/kv"
)

type Config struct{}

func NewFactory() kv.ProviderFactory {
	return func(raw json.RawMessage) (kv.Provider, error) {
		return nil, nil
	}
}
