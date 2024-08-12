package local

import (
	"context"
)

func (api *API) FlushAll(ctx context.Context) error {
	keyIndex, err := api.Store.Get(keyIndexKey)
	if err != nil {
		return err
	}

	keys := keyIndex.Value.(map[string]bool)
	for key := range keys {
		api.Delete(ctx, key)
	}

	api.Store.FlushAll()
	return nil
}
