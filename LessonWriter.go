package main

import (
	"context"
	"fmt"
	"github.com/go-redis/redis/v9"
	"github.com/kneu-messenger-pigeon/events"
	"strconv"
	"time"
)

type LessonWriter struct {
	redis       redis.UniversalClient
	maxLessonId MaxLessonIdSetterInterface
}

func (writer *LessonWriter) setRedis(redis redis.UniversalClient) {
	writer.redis = redis
}

func (writer *LessonWriter) getExpectedMessageKey() string {
	return events.LessonEventName
}

func (writer *LessonWriter) getExpectedEventType() any {
	return &events.LessonEvent{}
}

func (writer *LessonWriter) write(s any) error {
	event := s.(*events.LessonEvent)

	disciplineKey := fmt.Sprintf("%d:%d:lessons:%d", event.Year, event.Semester, event.DisciplineId)
	lessonKey := strconv.Itoa(int(event.Id))

	value := fmt.Sprintf("%s%d", event.Date.Format("060102"), event.TypeId)
	if event.IsDeleted {
		deletedLessonKey := fmt.Sprintf(
			"%d:%d:deleted-lessons:%d:%d",
			event.Year, event.Semester, event.DisciplineId, event.Id,
		)
		writer.redis.SetEx(context.Background(), deletedLessonKey, value, time.Hour*24)

		return writer.redis.HDel(context.Background(), disciplineKey, lessonKey).Err()
	} else {
		writer.maxLessonId.Set(event.Id)
		return writer.redis.HSet(context.Background(), disciplineKey, lessonKey, value).Err()
	}
}
