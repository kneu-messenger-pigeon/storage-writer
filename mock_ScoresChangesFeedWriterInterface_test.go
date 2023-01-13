// Code generated by mockery v2.14.1. DO NOT EDIT.

package main

import (
	context "context"

	events "github.com/kneu-messenger-pigeon/events"
	mock "github.com/stretchr/testify/mock"
)

// MockScoresChangesFeedWriterInterface is an autogenerated mock type for the ScoresChangesFeedWriterInterface type
type MockScoresChangesFeedWriterInterface struct {
	mock.Mock
}

// addToQueue provides a mock function with given fields: event
func (_m *MockScoresChangesFeedWriterInterface) addToQueue(event events.ScoreEvent) {
	_m.Called(event)
}

// execute provides a mock function with given fields: ctx
func (_m *MockScoresChangesFeedWriterInterface) execute(ctx context.Context) {
	_m.Called(ctx)
}

type mockConstructorTestingTNewMockScoresChangesFeedWriterInterface interface {
	mock.TestingT
	Cleanup(func())
}

// NewMockScoresChangesFeedWriterInterface creates a new instance of MockScoresChangesFeedWriterInterface. It also registers a testing interface on the mock and a cleanup function to assert the mocks expectations.
func NewMockScoresChangesFeedWriterInterface(t mockConstructorTestingTNewMockScoresChangesFeedWriterInterface) *MockScoresChangesFeedWriterInterface {
	mock := &MockScoresChangesFeedWriterInterface{}
	mock.Mock.Test(t)

	t.Cleanup(func() { mock.AssertExpectations(t) })

	return mock
}