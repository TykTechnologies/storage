package local

import (
	"context"
)

func (api *API) FlushAll(ctx context.Context) error {
	keyIndex, err := api.Store.Get(keyIndexKey)
	if err != nil {
		return err
	}

	keys := keyIndex.Value.(map[string]interface{})
	for key := range keys {
		err := api.Delete(ctx, key)
		if err != nil {
			return err
		}
	}

	// If supported
	//api.Store.FlushAll()
	return nil
}
