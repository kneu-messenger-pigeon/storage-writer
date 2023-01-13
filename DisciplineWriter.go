package main

import (
	"context"
	"fmt"
	"github.com/go-redis/redis/v9"
	"github.com/kneu-messenger-pigeon/events"
)

type DisciplineWriter struct {
	redis redis.UniversalClient
}

func (writer *DisciplineWriter) setRedis(redis redis.UniversalClient) {
	writer.redis = redis
}

func (writer *DisciplineWriter) getExpectedMessageKey() string {
	return events.DisciplineEventName
}

func (writer *DisciplineWriter) getExpectedEventType() any {
	return &events.DisciplineEvent{}
}

func (writer *DisciplineWriter) write(e interface{}) error {
	event := e.(*events.DisciplineEvent)

	key := fmt.Sprintf("%d:discipline:%d", event.Year, event.Id)
	if writer.redis.HGet(context.Background(), key, "origName").Val() != event.Name {
		return writer.redis.HSet(
			context.Background(), key,
			"name", clearDisciplineName(event.Name),
			"origName", event.Name,
		).Err()
	}

	return nil
}

func clearDisciplineName(name string) string {
	if name == "Фінанси (модуль 1 Гроші та кредит, модуль 2 Фінанси)" {
		return "Фінанси"
	}
	return name
}
