package main

import (
	"context"
	"fmt"
	"github.com/go-redis/redis/v9"
	"github.com/kneu-messenger-pigeon/events"
	"strconv"
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

	disciplineKey := fmt.Sprintf("%d:%d:lessons:%d", event.Year, event.Semester, event.DisciplineId)
	lessonKey := strconv.Itoa(int(event.Id))

	if event.IsDeleted {
		return writer.redis.HDel(context.Background(), disciplineKey, lessonKey).Err()
	} else {
		value := fmt.Sprintf("%s%d", event.Date.Format("060102"), event.TypeId)
		return writer.redis.HSet(context.Background(), disciplineKey, lessonKey, value).Err()
	}
}
