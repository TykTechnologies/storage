package local

func (api *API) addToKeyIndex(key string) error {
	o, err := api.Store.Get(keyIndexKey)
	if err != nil {
		// not found, create new
		o = &Object{
			Type:  TypeSet,
			Value: map[string]interface{}{key: true},
			NoExp: true,
		}

		return api.Store.Set(keyIndexKey, o)
	}

	if o == nil {
		o = &Object{
			Type:  TypeSet,
			Value: map[string]interface{}{key: true},
			NoExp: true,
		}
	}

	list := o.Value.(map[string]interface{})
	list[key] = true

	o.Value = list

	err = api.Store.Set(keyIndexKey, o)
	if err != nil {
		return err
	}

	return nil
}

func (api *API) updateDeletedKeysIndex(key string) error {
	o, err := api.Store.Get(deletedKeyIndexKey)
	if err != nil {
		// not found, create new
		o = &Object{
			Type:  TypeSet,
			Value: map[string]interface{}{key: true},
			NoExp: true,
		}

		return api.Store.Set(deletedKeyIndexKey, o)
	}

	if o == nil {
		o = &Object{
			Type:  TypeSet,
			Value: map[string]interface{}{key: true},
			NoExp: true,
		}
	}

	list := o.Value.(map[string]interface{})
	list[key] = true
	o.Value = list

	return api.Store.Set(deletedKeyIndexKey, o)
}
