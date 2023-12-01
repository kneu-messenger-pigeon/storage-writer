package main

import (
	"context"
	"github.com/go-redis/redis/v9"
)

type LessonExistCheckerInterface interface {
	Exists(year int, semester uint8, disciplineId uint, lessonId uint) bool
}

type LessonExistChecker struct {
	redis redis.UniversalClient
}

func newLessonExistChecker(redis redis.UniversalClient) *LessonExistChecker {
	return &LessonExistChecker{redis: redis}
}

func (checker *LessonExistChecker) Exists(year int, semester uint8, disciplineId uint, lessonId uint) bool {
	return checker.redis.HExists(
		context.Background(),
		getDisciplineKey(year, semester, disciplineId),
		getLessonKey(lessonId),
	).Val() || checker.redis.Exists(
		context.Background(),
		getDeletedLessonKey(year, semester, disciplineId, lessonId),
	).Val() == 1
}
