// Code generated by mockery v2.14.1. DO NOT EDIT.

package main

import (
	redis "github.com/redis/go-redis/v9"
	mock "github.com/stretchr/testify/mock"
)

// MockWriterInterface is an autogenerated mock type for the WriterInterface type
type MockWriterInterface struct {
	mock.Mock
}

// getExpectedEventType provides a mock function with given fields:
func (_m *MockWriterInterface) getExpectedEventType() interface{} {
	ret := _m.Called()

	var r0 interface{}
	if rf, ok := ret.Get(0).(func() interface{}); ok {
		r0 = rf()
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(interface{})
		}
	}

	return r0
}

// getExpectedMessageKey provides a mock function with given fields:
func (_m *MockWriterInterface) getExpectedMessageKey() string {
	ret := _m.Called()

	var r0 string
	if rf, ok := ret.Get(0).(func() string); ok {
		r0 = rf()
	} else {
		r0 = ret.Get(0).(string)
	}

	return r0
}

// setRedis provides a mock function with given fields: _a0
func (_m *MockWriterInterface) setRedis(_a0 redis.UniversalClient) {
	_m.Called(_a0)
}

// write provides a mock function with given fields: event
func (_m *MockWriterInterface) write(event interface{}) error {
	ret := _m.Called(event)

	var r0 error
	if rf, ok := ret.Get(0).(func(interface{}) error); ok {
		r0 = rf(event)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

type mockConstructorTestingTNewMockWriterInterface interface {
	mock.TestingT
	Cleanup(func())
}

// NewMockWriterInterface creates a new instance of MockWriterInterface. It also registers a testing interface on the mock and a cleanup function to assert the mocks expectations.
func NewMockWriterInterface(t mockConstructorTestingTNewMockWriterInterface) *MockWriterInterface {
	mock := &MockWriterInterface{}
	mock.Mock.Test(t)

	t.Cleanup(func() { mock.AssertExpectations(t) })

	return mock
}
