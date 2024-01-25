// Code generated by mockery v2.40.1. DO NOT EDIT.

package mocks

import (
	context "context"

	mock "github.com/stretchr/testify/mock"

	time "time"
)

// KeyValue is an autogenerated mock type for the KeyValue type
type KeyValue struct {
	mock.Mock
}

// Decrement provides a mock function with given fields: ctx, key
func (_m *KeyValue) Decrement(ctx context.Context, key string) (int64, error) {
	ret := _m.Called(ctx, key)

	if len(ret) == 0 {
		panic("no return value specified for Decrement")
	}

	var r0 int64
	var r1 error
	if rf, ok := ret.Get(0).(func(context.Context, string) (int64, error)); ok {
		return rf(ctx, key)
	}
	if rf, ok := ret.Get(0).(func(context.Context, string) int64); ok {
		r0 = rf(ctx, key)
	} else {
		r0 = ret.Get(0).(int64)
	}

	if rf, ok := ret.Get(1).(func(context.Context, string) error); ok {
		r1 = rf(ctx, key)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// Delete provides a mock function with given fields: ctx, key
func (_m *KeyValue) Delete(ctx context.Context, key string) error {
	ret := _m.Called(ctx, key)

	if len(ret) == 0 {
		panic("no return value specified for Delete")
	}

	var r0 error
	if rf, ok := ret.Get(0).(func(context.Context, string) error); ok {
		r0 = rf(ctx, key)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// DeleteKeys provides a mock function with given fields: ctx, keys
func (_m *KeyValue) DeleteKeys(ctx context.Context, keys []string) (int64, error) {
	ret := _m.Called(ctx, keys)

	if len(ret) == 0 {
		panic("no return value specified for DeleteKeys")
	}

	var r0 int64
	var r1 error
	if rf, ok := ret.Get(0).(func(context.Context, []string) (int64, error)); ok {
		return rf(ctx, keys)
	}
	if rf, ok := ret.Get(0).(func(context.Context, []string) int64); ok {
		r0 = rf(ctx, keys)
	} else {
		r0 = ret.Get(0).(int64)
	}

	if rf, ok := ret.Get(1).(func(context.Context, []string) error); ok {
		r1 = rf(ctx, keys)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// DeleteScanMatch provides a mock function with given fields: ctx, pattern
func (_m *KeyValue) DeleteScanMatch(ctx context.Context, pattern string) (int64, error) {
	ret := _m.Called(ctx, pattern)

	if len(ret) == 0 {
		panic("no return value specified for DeleteScanMatch")
	}

	var r0 int64
	var r1 error
	if rf, ok := ret.Get(0).(func(context.Context, string) (int64, error)); ok {
		return rf(ctx, pattern)
	}
	if rf, ok := ret.Get(0).(func(context.Context, string) int64); ok {
		r0 = rf(ctx, pattern)
	} else {
		r0 = ret.Get(0).(int64)
	}

	if rf, ok := ret.Get(1).(func(context.Context, string) error); ok {
		r1 = rf(ctx, pattern)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// Exists provides a mock function with given fields: ctx, key
func (_m *KeyValue) Exists(ctx context.Context, key string) (bool, error) {
	ret := _m.Called(ctx, key)

	if len(ret) == 0 {
		panic("no return value specified for Exists")
	}

	var r0 bool
	var r1 error
	if rf, ok := ret.Get(0).(func(context.Context, string) (bool, error)); ok {
		return rf(ctx, key)
	}
	if rf, ok := ret.Get(0).(func(context.Context, string) bool); ok {
		r0 = rf(ctx, key)
	} else {
		r0 = ret.Get(0).(bool)
	}

	if rf, ok := ret.Get(1).(func(context.Context, string) error); ok {
		r1 = rf(ctx, key)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// Expire provides a mock function with given fields: ctx, key, ttl
func (_m *KeyValue) Expire(ctx context.Context, key string, ttl time.Duration) error {
	ret := _m.Called(ctx, key, ttl)

	if len(ret) == 0 {
		panic("no return value specified for Expire")
	}

	var r0 error
	if rf, ok := ret.Get(0).(func(context.Context, string, time.Duration) error); ok {
		r0 = rf(ctx, key, ttl)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// Get provides a mock function with given fields: ctx, key
func (_m *KeyValue) Get(ctx context.Context, key string) (string, error) {
	ret := _m.Called(ctx, key)

	if len(ret) == 0 {
		panic("no return value specified for Get")
	}

	var r0 string
	var r1 error
	if rf, ok := ret.Get(0).(func(context.Context, string) (string, error)); ok {
		return rf(ctx, key)
	}
	if rf, ok := ret.Get(0).(func(context.Context, string) string); ok {
		r0 = rf(ctx, key)
	} else {
		r0 = ret.Get(0).(string)
	}

	if rf, ok := ret.Get(1).(func(context.Context, string) error); ok {
		r1 = rf(ctx, key)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// GetKeysAndValuesWithFilter provides a mock function with given fields: ctx, pattern
func (_m *KeyValue) GetKeysAndValuesWithFilter(ctx context.Context, pattern string) (map[string]interface{}, error) {
	ret := _m.Called(ctx, pattern)

	if len(ret) == 0 {
		panic("no return value specified for GetKeysAndValuesWithFilter")
	}

	var r0 map[string]interface{}
	var r1 error
	if rf, ok := ret.Get(0).(func(context.Context, string) (map[string]interface{}, error)); ok {
		return rf(ctx, pattern)
	}
	if rf, ok := ret.Get(0).(func(context.Context, string) map[string]interface{}); ok {
		r0 = rf(ctx, pattern)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(map[string]interface{})
		}
	}

	if rf, ok := ret.Get(1).(func(context.Context, string) error); ok {
		r1 = rf(ctx, pattern)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// GetKeysWithOpts provides a mock function with given fields: ctx, searchStr, cursors, count
func (_m *KeyValue) GetKeysWithOpts(ctx context.Context, searchStr string, cursors map[string]uint64, count int64) ([]string, map[string]uint64, bool, error) {
	ret := _m.Called(ctx, searchStr, cursors, count)

	if len(ret) == 0 {
		panic("no return value specified for GetKeysWithOpts")
	}

	var r0 []string
	var r1 map[string]uint64
	var r2 bool
	var r3 error
	if rf, ok := ret.Get(0).(func(context.Context, string, map[string]uint64, int64) ([]string, map[string]uint64, bool, error)); ok {
		return rf(ctx, searchStr, cursors, count)
	}
	if rf, ok := ret.Get(0).(func(context.Context, string, map[string]uint64, int64) []string); ok {
		r0 = rf(ctx, searchStr, cursors, count)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).([]string)
		}
	}

	if rf, ok := ret.Get(1).(func(context.Context, string, map[string]uint64, int64) map[string]uint64); ok {
		r1 = rf(ctx, searchStr, cursors, count)
	} else {
		if ret.Get(1) != nil {
			r1 = ret.Get(1).(map[string]uint64)
		}
	}

	if rf, ok := ret.Get(2).(func(context.Context, string, map[string]uint64, int64) bool); ok {
		r2 = rf(ctx, searchStr, cursors, count)
	} else {
		r2 = ret.Get(2).(bool)
	}

	if rf, ok := ret.Get(3).(func(context.Context, string, map[string]uint64, int64) error); ok {
		r3 = rf(ctx, searchStr, cursors, count)
	} else {
		r3 = ret.Error(3)
	}

	return r0, r1, r2, r3
}

// GetMulti provides a mock function with given fields: ctx, keys
func (_m *KeyValue) GetMulti(ctx context.Context, keys []string) ([]interface{}, error) {
	ret := _m.Called(ctx, keys)

	if len(ret) == 0 {
		panic("no return value specified for GetMulti")
	}

	var r0 []interface{}
	var r1 error
	if rf, ok := ret.Get(0).(func(context.Context, []string) ([]interface{}, error)); ok {
		return rf(ctx, keys)
	}
	if rf, ok := ret.Get(0).(func(context.Context, []string) []interface{}); ok {
		r0 = rf(ctx, keys)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).([]interface{})
		}
	}

	if rf, ok := ret.Get(1).(func(context.Context, []string) error); ok {
		r1 = rf(ctx, keys)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// Increment provides a mock function with given fields: ctx, key
func (_m *KeyValue) Increment(ctx context.Context, key string) (int64, error) {
	ret := _m.Called(ctx, key)

	if len(ret) == 0 {
		panic("no return value specified for Increment")
	}

	var r0 int64
	var r1 error
	if rf, ok := ret.Get(0).(func(context.Context, string) (int64, error)); ok {
		return rf(ctx, key)
	}
	if rf, ok := ret.Get(0).(func(context.Context, string) int64); ok {
		r0 = rf(ctx, key)
	} else {
		r0 = ret.Get(0).(int64)
	}

	if rf, ok := ret.Get(1).(func(context.Context, string) error); ok {
		r1 = rf(ctx, key)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// Keys provides a mock function with given fields: ctx, pattern
func (_m *KeyValue) Keys(ctx context.Context, pattern string) ([]string, error) {
	ret := _m.Called(ctx, pattern)

	if len(ret) == 0 {
		panic("no return value specified for Keys")
	}

	var r0 []string
	var r1 error
	if rf, ok := ret.Get(0).(func(context.Context, string) ([]string, error)); ok {
		return rf(ctx, pattern)
	}
	if rf, ok := ret.Get(0).(func(context.Context, string) []string); ok {
		r0 = rf(ctx, pattern)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).([]string)
		}
	}

	if rf, ok := ret.Get(1).(func(context.Context, string) error); ok {
		r1 = rf(ctx, pattern)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// Set provides a mock function with given fields: ctx, key, value, ttl
func (_m *KeyValue) Set(ctx context.Context, key string, value string, ttl time.Duration) error {
	ret := _m.Called(ctx, key, value, ttl)

	if len(ret) == 0 {
		panic("no return value specified for Set")
	}

	var r0 error
	if rf, ok := ret.Get(0).(func(context.Context, string, string, time.Duration) error); ok {
		r0 = rf(ctx, key, value, ttl)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// TTL provides a mock function with given fields: ctx, key
func (_m *KeyValue) TTL(ctx context.Context, key string) (int64, error) {
	ret := _m.Called(ctx, key)

	if len(ret) == 0 {
		panic("no return value specified for TTL")
	}

	var r0 int64
	var r1 error
	if rf, ok := ret.Get(0).(func(context.Context, string) (int64, error)); ok {
		return rf(ctx, key)
	}
	if rf, ok := ret.Get(0).(func(context.Context, string) int64); ok {
		r0 = rf(ctx, key)
	} else {
		r0 = ret.Get(0).(int64)
	}

	if rf, ok := ret.Get(1).(func(context.Context, string) error); ok {
		r1 = rf(ctx, key)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// NewKeyValue creates a new instance of KeyValue. It also registers a testing interface on the mock and a cleanup function to assert the mocks expectations.
// The first argument is typically a *testing.T value.
func NewKeyValue(t interface {
	mock.TestingT
	Cleanup(func())
}) *KeyValue {
	mock := &KeyValue{}
	mock.Mock.Test(t)

	t.Cleanup(func() { mock.AssertExpectations(t) })

	return mock
}
