package local

import (
	"context"
	"reflect"

	"github.com/TykTechnologies/storage/temporal/temperr"
)

func NewListObject(value []string) *Object {
	if value == nil {
		value = []string{}
	}
	return &Object{
		Type:  TypeList,
		Value: value,
		NoExp: true,
	}
}

// Remove the first count occurrences of elements equal to element from the list stored at key. If count is 0 remove all elements equal to element.
func (a *API) Remove(ctx context.Context, key string, count int64, iElement interface{}) (int64, error) {
	obj, err := a.Store.Get(key)
	if err != nil {
		return 0, err
	}

	if obj == nil || obj.Type != TypeList {
		return 0, nil
	}

	list := obj.Value.([]string)
	var removed int64
	var newList []string
	_, ok := iElement.([]byte)
	if !ok {
		return 0, temperr.KeyMisstype
	}

	element := string(iElement.([]byte))

	if count > 0 {
		// Remove from head to tail
		for _, item := range list {
			if removed < count && reflect.DeepEqual(item, element) {
				removed++
			} else {
				newList = append(newList, item)
			}
		}
	} else if count < 0 {
		// Remove from tail to head
		for i := len(list) - 1; i >= 0; i-- {
			if removed < -count && reflect.DeepEqual(list[i], element) {
				removed++
			} else {
				newList = append([]string{list[i]}, newList...)
			}
		}
	} else { // count == 0
		// Remove all occurrences
		for _, item := range list {
			if !reflect.DeepEqual(item, element) {
				newList = append(newList, item)
			}
		}
		removed = int64(len(list) - len(newList))
	}

	if removed > 0 {
		obj.Value = newList
		err = a.Store.Set(key, obj)
		if err != nil {
			return 0, err
		}
	}

	return removed, nil
}

func (api *API) Range(ctx context.Context, key string, start, stop int64) ([]string, error) {
	o, err := api.Store.Get(key)
	if err != nil {
		return nil, err
	}

	if o == nil || o.Type != TypeList {
		return nil, nil
	}

	list := o.Value.([]string)
	length := int64(len(list))

	// Convert negative indices to positive
	if start < 0 {
		if start < 0 {
			start = 0
		}
	}
	if stop < 0 {
		stop = length
	}

	// Ensure from is not greater than length
	if start >= length {
		return []string{}, nil
	}

	// Ensure to is not greater than length
	if stop >= length {
		stop = length - 1
	}

	// Ensure from is not greater than to
	if start > stop {
		return []string{}, nil
	}

	// +1 because slicing in Go is exclusive for the upper bound
	return list[start : stop+1], nil
}

// Returns the length of the list stored at key.
func (api *API) Length(ctx context.Context, key string) (int64, error) {
	o, err := api.Store.Get(key)
	if err != nil {
		return 0, err
	}

	if o == nil || o.Type != TypeList {
		return 0, nil
	}

	return int64(len(o.Value.([]string))), nil
}

// Insert all the specified values at the head of the list stored at key.
// If key does not exist, it is created.
// pipelined: If true, the operation is pipelined and executed in a single roundtrip.
func (api *API) Prepend(ctx context.Context, pipelined bool, key string, values ...[]byte) error {
	o, err := api.Store.Get(key)
	if err != nil {
		return err
	}

	// reverse the vlaues
	for i, j := 0, len(values)-1; i < j; i, j = i+1, j-1 {
		values[i], values[j] = values[j], values[i]
	}

	if o == nil {
		l := make([]string, len(values))
		for i, value := range values {
			l[i] = string(value)
		}

		o = NewListObject(l)
		api.Store.Set(key, o)
		return nil
	}

	if o.Type != TypeList {
		return temperr.KeyMisstype
	}

	// values is in order, but needs to be inserted in reverse order
	for i := len(values) - 1; i >= 0; i-- {
		o.Value = append([]string{string(values[i])}, o.Value.([]string)...)
	}

	api.Store.Set(key, o)
	return nil
}

func (api *API) Append(ctx context.Context, pipelined bool, key string, values ...[]byte) error {
	o, err := api.Store.Get(key)
	if err != nil {
		return err
	}

	if o == nil {
		l := make([]string, len(values))
		for i, value := range values {
			l[i] = string(value)
		}
		o = NewListObject(l)
		api.Store.Set(key, o)
		return nil
	}

	if o.Type != TypeList {
		return temperr.KeyMisstype
	}

	for _, value := range values {
		o.Value = append(o.Value.([]string), string(value))
	}

	api.Store.Set(key, o)
	return nil
}

// Pop removes and returns the first count elements of the list stored at key.
// If stop is -1, all the elements from start to the end of the list are removed and returned.
func (api *API) Pop(ctx context.Context, key string, stop int64) ([]string, error) {
	o, err := api.Store.Get(key)
	if err != nil {
		return nil, err
	}

	if o == nil || o.Type != TypeList {
		return nil, nil
	}

	list := o.Value.([]string)
	length := int64(len(list))

	var incl int64 = 0
	if stop == -1 {
		stop = length
		incl = 1
	}

	if stop >= length {
		stop = length - 1
	}

	if stop < 0 {
		return []string{}, nil
	}

	popped := list[:stop+incl]
	o.Value = list[stop+incl:]
	api.Store.Set(key, o)
	return popped, nil
}
