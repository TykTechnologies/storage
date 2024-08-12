package local

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/TykTechnologies/storage/temporal/temperr"
)

func (api *API) Get(ctx context.Context, key string) (value string, err error) {
	if key == "" {
		return "", temperr.KeyEmpty
	}

	o, err := api.Store.Get(key)
	if err != nil {
		return "", temperr.KeyNotFound
	}

	if o == nil {
		return "", temperr.KeyNotFound
	}

	if o.IsExpired() {
		return "", temperr.KeyNotFound
	}

	if o.Deleted {
		return "", temperr.KeyNotFound
	}

	v, ok := o.Value.(string)
	if !ok {
		switch o.Value.(type) {
		case int:
			v = strconv.Itoa(o.Value.(int))
		case int64:
			v = strconv.FormatInt(o.Value.(int64), 10)
		case int32:
			v = strconv.FormatInt(int64(o.Value.(int32)), 10)
		default:
			return "", temperr.KeyMisstype
		}
	}

	return v, nil
}

func (api *API) Set(ctx context.Context, key, value string, ttl time.Duration) error {
	if key == "" {
		return temperr.KeyEmpty
	}

	o := &Object{
		Value: value,
	}

	o.NoExp = true
	if ttl > 0 {
		o.SetExpire(ttl)
		o.NoExp = false
	}

	err := api.Store.Set(key, o)
	if err != nil {
		return err
	}

	return api.addToKeyIndex(key)
}

func (api *API) SetIfNotExist(ctx context.Context, key, value string, expiration time.Duration) (bool, error) {
	if key == "" {
		return false, temperr.KeyEmpty
	}

	o, err := api.Store.Get(key)
	if err != nil {
		return false, err
	}

	if o != nil {
		if !o.Deleted && !o.IsExpired() {
			return false, nil
		}
	}

	err = api.Set(ctx, key, value, expiration)
	if err != nil {
		return false, err
	}

	err = api.addToKeyIndex(key)
	if err != nil {
		return false, err
	}

	return true, nil
}

func (api *API) Delete(ctx context.Context, key string) error {
	if key == "" {
		return temperr.KeyEmpty
	}

	o, err := api.Store.Get(key)
	if err != nil {
		return err
	}

	if o == nil {
		return nil
	}

	o.Deleted = true
	o.DeletedAt = time.Now()

	err = api.Store.Set(key, o)
	if err != nil {
		return err
	}

	return api.updateDeletedKeysIndex(key)
}

func (api *API) Increment(ctx context.Context, key string) (int64, error) {
	if key == "" {
		return 0, temperr.KeyEmpty
	}

	o, err := api.Store.Get(key)
	if err != nil {
		// create the object
		o = &Object{
			Value: int64(1),
			Type:  TypeCounter,
			NoExp: true,
		}

		api.Store.Set(key, o)
		api.addToKeyIndex(key)
		return 1, nil
	}

	if o == nil {
		o = &Object{
			Value: int64(1),
			Type:  TypeCounter,
			NoExp: true,
		}

		api.Store.Set(key, o)
		api.addToKeyIndex(key)
		return 1, nil
	}

	if o.Deleted || o.IsExpired() {
		o = &Object{
			Value: int64(0),
			Type:  TypeCounter,
			NoExp: true,
		}
	}

	var v int64 = -1
	if o.Type != TypeCounter {
		switch o.Value.(type) {
		case int:
			fmt.Println("int")
			o.Type = TypeCounter
			v = int64(o.Value.(int))
		case int64:
			fmt.Println("int64")
			o.Type = TypeCounter
			v = o.Value.(int64)
		case int32:
			fmt.Println("int32")
			o.Type = TypeCounter
			v = int64(o.Value.(int32))
		case string:
			fmt.Println("string")
			// try to convert
			conv, err := strconv.Atoi(o.Value.(string))
			if err != nil {
				return 0, temperr.KeyMisstype
			}
			o.Value = int64(conv)
			v = int64(conv)
			o.Type = TypeCounter
		default:
			return 0, temperr.KeyMisstype
		}
	} else {
		var ok bool
		v, ok = o.Value.(int64)
		if !ok {
			return 0, temperr.KeyMisstype
		}
	}

	o.Value = v + 1
	err = api.Store.Set(key, o)
	if err != nil {
		return 0, err
	}

	return o.Value.(int64), nil
}

func (api *API) Decrement(ctx context.Context, key string) (newValue int64, err error) {
	if key == "" {
		return 0, temperr.KeyEmpty
	}

	o, err := api.Store.Get(key)
	if err != nil {
		// create the object
		o = &Object{
			Value: int64(-1),
			Type:  TypeCounter,
			NoExp: true,
		}

		api.Store.Set(key, o)
		api.addToKeyIndex(key)
		return -1, nil
	}

	if o == nil {
		o = &Object{
			Value: int64(-1),
			Type:  TypeCounter,
			NoExp: true,
		}

		api.Store.Set(key, o)
		api.addToKeyIndex(key)
		return -1, nil
	}

	if o.Deleted || o.IsExpired() {
		o = &Object{
			Value: int64(0),
			Type:  TypeCounter,
			NoExp: true,
		}
	}

	var v int64
	if o.Type != TypeCounter {
		switch o.Value.(type) {
		case int:
			o.Type = TypeCounter
			v = int64(o.Value.(int))
		case int64:
			o.Type = TypeCounter
			v = o.Value.(int64)
		case int32:
			o.Type = TypeCounter
			v = int64(o.Value.(int32))
		case string:
			// try to convert
			conv, err := strconv.Atoi(o.Value.(string))
			if err != nil {
				return 0, temperr.KeyMisstype
			}
			o.Value = int64(conv)
			v = int64(conv)
			o.Type = TypeCounter
		default:
			return 0, temperr.KeyMisstype
		}
	} else {
		var ok bool
		v, ok = o.Value.(int64)
		if !ok {
			return 0, temperr.KeyMisstype
		}
	}

	o.Value = v - 1

	err = api.Store.Set(key, o)
	if err != nil {
		return 0, err
	}

	return o.Value.(int64), nil
}

func (api *API) Exists(ctx context.Context, key string) (exists bool, err error) {
	if key == "" {
		return false, temperr.KeyEmpty
	}

	o, err := api.Get(ctx, key)
	if err != nil {
		return false, nil
	}

	if o == "" {
		return false, nil
	}

	return true, nil
}

func (api *API) Expire(ctx context.Context, key string, ttl time.Duration) error {
	if key == "" {
		return temperr.KeyEmpty
	}

	o, err := api.Store.Get(key)
	if err != nil {
		return err
	}

	if o == nil {
		return nil
	}

	if ttl <= 0 {
		o.NoExp = true
		return nil
	}

	o.SetExpire(ttl)
	o.NoExp = false

	return api.Store.Set(key, o)
}

func (api *API) TTL(ctx context.Context, key string) (ttl int64, err error) {
	if key == "" {
		return -2, temperr.KeyEmpty
	}

	o, err := api.Store.Get(key)
	if err != nil {
		return -2, err
	}

	if o == nil {
		return -2, nil
	}

	if o.NoExp {
		return -1, nil
	}

	return int64(time.Until(o.Exp).Round(time.Second).Seconds()), nil
}

func (api *API) DeleteKeys(ctx context.Context, keys []string) (numberOfDeletedKeys int64, err error) {
	if len(keys) == 0 {
		return 0, temperr.KeyEmpty
	}
	var k int64 = 0
	for _, key := range keys {
		e, _ := api.Exists(ctx, key)
		if e {
			err = api.Delete(ctx, key)
			if err != nil {
				return k, err
			}
			k++
		}

	}

	return k, nil
}

func (api *API) DeleteScanMatch(ctx context.Context, pattern string) (int64, error) {
	err := api.Connector.Ping(ctx)
	if err != nil {
		return 0, err
	}

	keys, err := api.Keys(ctx, pattern)
	var k int64 = 0
	if err != nil {
		return k, err
	}

	for _, key := range keys {
		err := api.Delete(ctx, key)
		if err != nil {
			return k, err
		}
		k++
	}

	return k, nil
}

func (api *API) Keys(ctx context.Context, pattern string) ([]string, error) {
	err := api.Connector.Ping(ctx)
	if err != nil {
		return nil, err
	}
	// filter is a prefix, e.g. rumbaba:keys:*
	// strip the *
	// Strip the trailing "*" from the pattern
	pattern = strings.TrimSuffix(pattern, "*")

	// Get the key index
	keyIndexObj, err := api.Store.Get(keyIndexKey)
	if err != nil {
		return nil, err
	}
	if keyIndexObj == nil {
		return nil, nil
	}
	keyIndex := keyIndexObj.Value.(map[string]interface{})

	// Get the deleted key index
	deletedKeyIndexObj, err := api.Store.Get(deletedKeyIndexKey)
	if err != nil {
		return nil, err
	}
	var deletedKeys map[string]bool
	if deletedKeyIndexObj != nil {
		deletedKeysList := deletedKeyIndexObj.Value.(map[string]interface{})
		deletedKeys = make(map[string]bool, len(deletedKeysList))
		for key := range deletedKeysList {
			deletedKeys[key] = true
		}
	} else {
		deletedKeys = make(map[string]bool)
	}

	var retKeys []string
	for key := range keyIndex {
		// Check if the key matches the pattern and is not deleted
		if (pattern == "" || strings.HasPrefix(key, pattern)) && !deletedKeys[key] {
			retKeys = append(retKeys, key)
		}
	}

	return retKeys, nil
}

func (api *API) GetMulti(ctx context.Context, keys []string) (values []interface{}, err error) {
	var objects []interface{}
	for _, key := range keys {
		o, _ := api.Get(ctx, key)

		if o == "" {
			objects = append(objects, nil)
			continue
		}

		objects = append(objects, o)

	}

	return objects, nil
}

func (api *API) GetKeysAndValuesWithFilter(ctx context.Context, pattern string) (keysAndValues map[string]interface{}, err error) {
	keys, err := api.Keys(ctx, pattern)
	if err != nil {
		return nil, err
	}

	kv := make(map[string]interface{})

	for _, key := range keys {
		o, err := api.Get(ctx, key)
		if err != nil {
			continue
		}

		kv[key] = o
	}

	return kv, nil
}

func (api *API) GetKeysWithOpts(ctx context.Context, searchStr string, cursors map[string]uint64,
	count int64) (keys []string, updatedCursor map[string]uint64, continueScan bool, err error) {

	err = api.Connector.Ping(ctx)
	if err != nil {
		return nil, nil, false, err
	}

	// no op
	return nil, nil, true, nil
}
