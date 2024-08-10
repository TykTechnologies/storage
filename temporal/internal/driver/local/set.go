package local

import (
	"context"
	"errors"

	"github.com/TykTechnologies/storage/temporal/temperr"
)

func NewSetObject(value []interface{}) *Object {
	return &Object{
		Type:  TypeSet,
		Value: value,
		NoExp: true,
	}
}

func (api *API) Members(ctx context.Context, key string) ([]string, error) {
	if key == "" {
		return nil, temperr.KeyEmpty
	}

	o, err := api.Store.Get(key)
	if err != nil {
		return nil, err
	}

	if o == nil {
		return []string{}, nil
	}

	if o.Deleted || o.IsExpired() {
		return []string{}, nil
	}

	if o.Type != TypeSet {
		return nil, errors.New("key not a valid set")
	}

	set := make([]string, 0)
	if o.Value == nil {
		return set, nil
	}

	for _, v := range o.Value.([]interface{}) {
		vs, ok := v.(string)
		if !ok {
			return nil, errors.New("invalid set member")
		}

		set = append(set, vs)
	}

	return set, nil
}

func (api *API) AddMember(ctx context.Context, key, member string) error {
	if key == "" {
		return temperr.KeyEmpty
	}

	o, err := api.Store.Get(key)
	if err != nil {
		o = NewSetObject([]interface{}{member})
		api.Store.Set(key, o)
	}

	if o == nil {
		o = NewSetObject([]interface{}{member})
		api.Store.Set(key, o)
		return nil
	}

	if o.Type != TypeSet || o.Deleted || o.IsExpired() {
		return temperr.KeyNotFound
	}

	o.Value = append(o.Value.([]interface{}), member)

	err = api.Store.Set(key, o)
	if err != nil {
		return err
	}

	return nil
}

func (api *API) RemoveMember(ctx context.Context, key, member string) error {
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

	if o.Type != TypeSet || o.Deleted || o.IsExpired() {
		return errors.New("key not a valid set")
	}

	var newSet []interface{}
	for _, v := range o.Value.([]interface{}) {
		if v == member {
			continue
		}

		newSet = append(newSet, v)
	}

	o.Value = newSet

	err = api.Store.Set(key, o)
	if err != nil {
		return err
	}

	return nil
}

func (api *API) IsMember(ctx context.Context, key, member string) (bool, error) {
	if key == "" {
		return false, temperr.KeyEmpty
	}

	o, err := api.Store.Get(key)
	if err != nil {
		return false, err
	}

	if o == nil {
		return false, nil
	}

	if o.Type != TypeSet || o.Deleted || o.IsExpired() {
		return false, errors.New("key not a valid set")
	}

	for _, v := range o.Value.([]interface{}) {
		if v == member {
			return true, nil
		}
	}

	return false, nil
}
