// Code generated by mockery v2.40.1. DO NOT EDIT.

package mocks

import (
	model "github.com/TykTechnologies/storage/temporal/model"
	mock "github.com/stretchr/testify/mock"
)

// Option is an autogenerated mock type for the Option type
type Option struct {
	mock.Mock
}

// Apply provides a mock function with given fields: _a0
func (_m *Option) Apply(_a0 *model.BaseConfig) {
	_m.Called(_a0)
}

// NewOption creates a new instance of Option. It also registers a testing interface on the mock and a cleanup function to assert the mocks expectations.
// The first argument is typically a *testing.T value.
func NewOption(t interface {
	mock.TestingT
	Cleanup(func())
}) *Option {
	mock := &Option{}
	mock.Mock.Test(t)

	t.Cleanup(func() { mock.AssertExpectations(t) })

	return mock
}
