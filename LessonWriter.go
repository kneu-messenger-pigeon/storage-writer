package main

import (
	"context"
	"fmt"
	"github.com/go-redis/redis/v9"
	"github.com/kneu-messenger-pigeon/events"
	"time"
)

type LessonWriter struct {
	redis redis.UniversalClient
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

	disciplineKey := getDisciplineKey(event.Year, event.Semester, event.DisciplineId)
	lessonKey := getLessonKey(event.Id)

	value := fmt.Sprintf("%s%d", event.Date.Format("060102"), event.TypeId)
	if event.IsDeleted {
		deletedLessonKey := getDeletedLessonKey(event.Year, event.Semester, event.DisciplineId, event.Id)
		writer.redis.SetEx(context.Background(), deletedLessonKey, value, time.Hour*24)

		return writer.redis.HDel(context.Background(), disciplineKey, lessonKey).Err()
	} else {
		err := writer.redis.HSet(context.Background(), disciplineKey, lessonKey, value).Err()
		return err
	}
}
