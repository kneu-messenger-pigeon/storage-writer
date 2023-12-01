package main

import (
	"github.com/go-redis/redismock/v9"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestLessonExistChecker_Exists(t *testing.T) {
	redis, redisMock := redismock.NewClientMock()

	redisMock.ExpectHExists(
		getDisciplineKey(2024, 1, 235),
		getLessonKey(4120),
	).SetVal(true)

	redisMock.ExpectHExists(
		getDisciplineKey(2024, 2, 645),
		getLessonKey(5780),
	).SetVal(true)

	redisMock.ExpectHExists(
		getDisciplineKey(2030, 1, 980),
		getLessonKey(6500),
	).SetVal(false)

	checker := newLessonExistChecker(redis)

	assert.True(t, checker.Exists(2024, 1, 235, 4120))
	assert.True(t, checker.Exists(2024, 2, 645, 5780))
	assert.False(t, checker.Exists(2030, 1, 980, 6500))
}
