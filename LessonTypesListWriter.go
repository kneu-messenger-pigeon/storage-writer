package main

import (
	"bytes"
	"context"
	"encoding/json"
	"github.com/go-redis/redis/v9"
	"github.com/kneu-messenger-pigeon/events"
	"io"
)

type LessonTypesListWriter struct {
	out   io.Writer
	redis redis.UniversalClient
}

func (writer *LessonTypesListWriter) setRedis(redis redis.UniversalClient) {
	writer.redis = redis
}

func (writer *LessonTypesListWriter) getExpectedMessageKey() string {
	return events.LessonTypesListName
}

func (writer *LessonTypesListWriter) getExpectedEventType() any {
	return &events.LessonTypesList{}
}

func (writer *LessonTypesListWriter) write(s any) (err error) {
	lessonTypesList := s.(*events.LessonTypesList).List
	serializedList, _ := json.Marshal(lessonTypesList)
	prevSerializedList, err := writer.redis.GetSet(context.Background(), "lessonTypes", serializedList).Bytes()
	if err == redis.Nil {
		err = nil
		prevSerializedList = make([]byte, 0)
	}
	if err == nil && !bytes.Equal(serializedList, prevSerializedList) {
		err = writer.redis.BgSave(context.Background()).Err()
	}
	return
}
