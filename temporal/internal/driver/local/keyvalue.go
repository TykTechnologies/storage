package local

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/TykTechnologies/storage/temporal/temperr"
)

func (api *API) Get(ctx context.Context, key string) (string, error) {
	if key == "" {
		return "", temperr.KeyEmpty
	}

	o, err := api.Store.Get(key)
	if err != nil {
		return "", err
	}

	if o == nil || o.IsExpired() || o.Deleted {
		return "", temperr.KeyNotFound
	}

	return api.convertToString(o.Value)
}

func (api *API) convertToString(value interface{}) (string, error) {
	switch v := value.(type) {
	case string:
		return v, nil
	case int:
		return strconv.Itoa(v), nil
	case int64:
		return strconv.FormatInt(v, 10), nil
	case int32:
		return strconv.FormatInt(int64(v), 10), nil
	default:
		return "", temperr.KeyMisstype
	}
}

func (api *API) Set(ctx context.Context, key, value string, ttl time.Duration) error {
	if key == "" {
		return temperr.KeyEmpty
	}

	o := &Object{
		Value: value,
		NoExp: ttl <= 0,
	}

	if !o.NoExp {
		o.SetExpire(ttl)
	}

	if err := api.Store.Set(key, o); err != nil {
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

	if o != nil && !o.Deleted && !o.IsExpired() {
		return false, nil
	}

	if err := api.Set(ctx, key, value, expiration); err != nil {
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
		return nil // Key doesn't exist, no need to delete
	}

	// Check if hard delete is supported by the store
	_, delSupport := api.Store.Features()[FeatureHardDelete]
	if delSupport {
		return api.Store.Delete(key)
	}

	o.Deleted = true
	o.DeletedAt = time.Now()
	o.Value = "" // empty the value to save memory

	if err := api.Store.Set(key, o); err != nil {
		return err
	}

	return api.updateDeletedKeysIndex(key)
}

func NewCounter(value int64) *Object {
	return &Object{
		Value: value,
		Type:  TypeCounter,
		NoExp: true,
	}
}

func (api *API) Increment(ctx context.Context, key string) (int64, error) {
	if key == "" {
		return 0, temperr.KeyEmpty
	}

	o, err := api.Store.Get(key)
	if err != nil || o == nil || o.Deleted || o.IsExpired() {
		return api.createNewCounter(key)
	}

	value, err := api.getCounterValue(o)
	if err != nil {
		return 0, err
	}

	newValue := value + 1
	o.Value = newValue
	o.Type = TypeCounter

	if err := api.Store.Set(key, o); err != nil {
		return 0, err
	}

	return newValue, nil
}

func (api *API) createNewCounter(key string) (int64, error) {
	o := NewCounter(1)
	if err := api.Store.Set(key, o); err != nil {
		return 0, err
	}
	if err := api.addToKeyIndex(key); err != nil {
		return 0, err
	}
	return 1, nil
}

func (api *API) getCounterValue(o *Object) (int64, error) {
	switch v := o.Value.(type) {
	case int:
		return int64(v), nil
	case int64:
		return v, nil
	case int32:
		return int64(v), nil
	case string:
		i, err := strconv.Atoi(v)
		if err != nil {
			return 0, temperr.KeyMisstype
		}
		return int64(i), err
	default:
		return 0, temperr.KeyMisstype
	}
}

func (api *API) Decrement(ctx context.Context, key string) (int64, error) {
	if key == "" {
		return 0, temperr.KeyEmpty
	}

	o, err := api.Store.Get(key)
	if err != nil || o == nil || o.Deleted || o.IsExpired() {
		return api.createNewCounterWithValue(key, -1)
	}

	value, err := api.getCounterValue(o)
	if err != nil {
		return 0, err
	}

	newValue := value - 1
	o.Value = newValue
	o.Type = TypeCounter

	if err := api.Store.Set(key, o); err != nil {
		return 0, err
	}

	return newValue, nil
}

func (api *API) createNewCounterWithValue(key string, value int64) (int64, error) {
	o := NewCounter(value)
	if err := api.Store.Set(key, o); err != nil {
		return 0, err
	}
	if err := api.addToKeyIndex(key); err != nil {
		return 0, err
	}
	return value, nil
}

func (api *API) Exists(ctx context.Context, key string) (bool, error) {
	if key == "" {
		return false, temperr.KeyEmpty
	}

	_, err := api.Get(ctx, key)
	if err == nil {
		return true, nil
	}
	if err == temperr.KeyNotFound {
		return false, nil
	}
	return false, err
}

func (api *API) Expire(ctx context.Context, key string, ttl time.Duration) error {
	if key == "" {
		return temperr.KeyEmpty
	}

	// non-existing keys for these functions should return nil, not errors
	o, err := api.Store.Get(key)
	if err != nil {
		return nil
	}
	if o == nil {
		return nil
	}

	if ttl <= 0 {
		o.NoExp = true
	} else {
		o.SetExpire(ttl)
		o.NoExp = false
	}

	return api.Store.Set(key, o)
}

func (api *API) TTL(ctx context.Context, key string) (int64, error) {
	if key == "" {
		return -2, temperr.KeyEmpty
	}

	o, err := api.Store.Get(key)
	if err != nil {
		// bizarre, but should return nil
		return -2, nil
	}
	if o == nil {
		return -2, nil
	}

	if o.NoExp {
		return -1, nil
	}

	ttl := time.Until(o.Exp).Round(time.Second).Seconds()
	return int64(ttl), nil
}

func (api *API) DeleteKeys(ctx context.Context, keys []string) (int64, error) {
	if len(keys) == 0 {
		return 0, temperr.KeyEmpty
	}

	var deleted int64
	for _, key := range keys {
		exists, err := api.Exists(ctx, key)
		if err != nil {
			return deleted, err
		}
		if exists {
			if err := api.Delete(ctx, key); err != nil {
				return deleted, err
			}
			deleted++
		}
	}

	return deleted, nil
}

func (api *API) DeleteScanMatch(ctx context.Context, pattern string) (int64, error) {
	if err := api.Connector.Ping(ctx); err != nil {
		return 0, err
	}

	keys, err := api.Keys(ctx, pattern)
	if err != nil {
		return 0, err
	}

	// need to return nil for this function
	c, err := api.DeleteKeys(ctx, keys)
	if err != nil {
		return 0, nil
	}

	return c, nil
}

func (api *API) Keys(ctx context.Context, pattern string) ([]string, error) {
	if err := api.Connector.Ping(ctx); err != nil {
		return nil, err
	}

	keyIndexObj, err := api.Store.Get(keyIndexKey)
	if err != nil {
		return nil, err
	}
	if keyIndexObj == nil {
		return nil, nil
	}

	keyIndex, ok := keyIndexObj.Value.(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("invalid key index format")
	}

	deletedKeyIndexObj, err := api.Store.Get(deletedKeyIndexKey)
	if err != nil {
		return nil, err
	}
	deletedKeys, ok := deletedKeyIndexObj.Value.(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("invalid deleted key index format")
	}

	pattern = strings.TrimSuffix(pattern, "*")
	var matchedKeys []string

	for key := range keyIndex {
		if !api.isKeyDeleted(key, deletedKeys) && strings.HasPrefix(key, pattern) {
			matchedKeys = append(matchedKeys, key)
		}
	}

	return matchedKeys, nil
}

func (api *API) isKeyDeleted(key string, deletedKeys map[string]interface{}) bool {
	_, deleted := deletedKeys[key]
	return deleted
}

func (api *API) GetMulti(ctx context.Context, keys []string) ([]interface{}, error) {
	var values []interface{}

	for _, key := range keys {
		value, err := api.Get(ctx, key)
		if err == temperr.KeyNotFound {
			values = append(values, nil)
		} else if err != nil {
			return nil, err
		} else {
			values = append(values, value)
		}
	}

	return values, nil
}

func (api *API) GetKeysAndValuesWithFilter(ctx context.Context, pattern string) (map[string]interface{}, error) {
	keys, err := api.Keys(ctx, pattern)
	if err != nil {
		return nil, err
	}

	keysAndValues := make(map[string]interface{})

	for _, key := range keys {
		value, err := api.Get(ctx, key)
		if err == nil {
			keysAndValues[key] = value
		} else if err != temperr.KeyNotFound {
			return nil, err
		}
	}

	return keysAndValues, nil
}

func (api *API) GetKeysWithOpts(ctx context.Context, searchStr string, cursors map[string]uint64,
	count int64) (keys []string, updatedCursor map[string]uint64, continueScan bool, err error) {

	if err := api.Connector.Ping(ctx); err != nil {
		return nil, nil, false, err
	}

	// TODO: Implement the actual functionality based on your requirements
	// This function is currently a no-op and needs to be implemented

	return nil, nil, true, nil
}
