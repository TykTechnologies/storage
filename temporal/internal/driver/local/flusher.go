package local

import (
	"context"
)

func (api *API) FlushAll(ctx context.Context) error {
	// save the ops
	_, ok := api.Store.Features()[FeatureFlushAll]
	if ok {
		err := api.Store.FlushAll()
		if err != nil {
			return err
		}

		api.initialiseKeyIndexes()
	}

	keyIndex, err := api.Store.Get(keyIndexKey)
	if err != nil {
		return err
	}

	keys := keyIndex.Value.(map[string]interface{})
	for key := range keys {
		err := api.deleteWithOptions(ctx, key, true)
		if err != nil {
			return err
		}
	}

	api.initialiseKeyIndexes()
	return nil
}
