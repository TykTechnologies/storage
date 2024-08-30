package local

import (
	"fmt"
	"log/slog"
	"time"
)

func (api *API) addOrUpdateTTLIndex(keyName string, obj *Object) error {
	o, err := api.Store.Get(ttlIndexKey)
	if err != nil {
		// not found, create new
		o = &Object{
			Type:  TypeSet,
			Value: map[string]interface{}{keyName: obj.Exp},
			NoExp: true,
		}

		return api.Store.Set(ttlIndexKey, o)
	}

	if o == nil {
		o = &Object{
			Type:  TypeSet,
			Value: map[string]interface{}{keyName: obj.Exp},
			NoExp: true,
		}
		return api.Store.Set(ttlIndexKey, o)
	}

	v, ok := o.Value.(map[string]interface{})
	if !ok {
		return fmt.Errorf("invalid type for ttl index")
	}

	v[keyName] = obj.Exp
	o.Value = v

	return api.Store.Set(ttlIndexKey, o)
}

func (api *API) walkTTLIndex() {
	return // disable for now (there are distribution issues)
	for {
		if api.Stopped {
			return
		}
		o, err := api.Store.Get(ttlIndexKey)
		if err != nil {
			continue
		}

		if o == nil {
			continue
		}

		v, ok := o.Value.(map[string]interface{})
		if !ok {
			continue
		}

		for key, exp := range v {
			// This might not work
			parsedExp, err := getTime(exp)
			if err != nil {
				slog.Error("ttl index value is not a time", "error", err)
				continue
			}

			if parsedExp.After(time.Now()) {
				// fmt.Println("purging TTL key", key, "expiry was: ", parsedExp)
				// api.Store.Delete(key)
				api.updateDeletedKeysIndex(key)
				delete(o.Value.(map[string]interface{}), key)
			}
		}

		api.Store.Set(ttlIndexKey, o)
		time.Sleep(5 * time.Second)
	}

}

var defaulttimeFormat = "2006-01-02 15:04:05.999999999 -0700 MST"

func getTime(obj interface{}) (time.Time, error) {
	switch v := obj.(type) {
	case time.Time:
		return v, nil
	case interface{}:
		str, ok := v.(string)
		if !ok {
			return time.Time{}, fmt.Errorf("invalid type for time (from interface{})")
		}

		t, err := time.Parse(defaulttimeFormat, str)
		if err != nil {
			return time.Time{}, fmt.Errorf("invalid time format: ", err)
		}

		return t, nil
	case string:
		t, err := time.Parse(defaulttimeFormat, v)
		if err != nil {
			return time.Time{}, fmt.Errorf("invalid time format (from string): ", err)
		}

		return t, nil

	default:
		return time.Time{}, fmt.Errorf("invalid type for time: %T", v)
	}
}

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

	// Check the delete index and update it, this is in case a key is re-created
	o, err = api.Store.Get(deletedKeyIndexKey)
	if err != nil {
		// not found, create new
		return err
	}

	if o == nil {
		return fmt.Errorf("deletedKeyIndex is nil!")
	}

	list = o.Value.(map[string]interface{})
	if _, ok := list[key]; ok {
		delete(list, key)
		o.Value = list
		return api.Store.Set(deletedKeyIndexKey, o)
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
