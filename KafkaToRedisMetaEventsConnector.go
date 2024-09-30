package main

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/kneu-messenger-pigeon/events"
	"github.com/redis/go-redis/v9"
	"github.com/segmentio/kafka-go"
	"io"
	"sync"
)

type KafkaToRedisMetaEventsConnector struct {
	out                   io.Writer
	reader                events.ReaderInterface
	redis                 redis.UniversalClient
	currentYearWriter     WriterInterface
	lessonTypesListWriter WriterInterface
}

func (connector *KafkaToRedisMetaEventsConnector) execute(ctx context.Context, wg *sync.WaitGroup) {
	var err error
	var message kafka.Message
	var messageKey string
	currentYearEvent := &events.CurrentYearEvent{}
	lessonTypeList := &events.LessonTypesList{}

	if connector.currentYearWriter == nil || connector.lessonTypesListWriter == nil {
		wg.Done()
		return
	}

	connector.currentYearWriter.setRedis(connector.redis)
	connector.lessonTypesListWriter.setRedis(connector.redis)

	fmt.Fprintf(connector.out, "%T connector started \n", connector)

	for ctx.Err() == nil {
		message, err = connector.reader.FetchMessage(ctx)
		messageKey = string(message.Key)
		if err == nil {
			if messageKey == events.CurrentYearEventName {
				err = json.Unmarshal(message.Value, &currentYearEvent)
				if err == nil {
					err = connector.currentYearWriter.write(currentYearEvent)
				}
			} else if messageKey == events.LessonTypesListName {
				err = json.Unmarshal(message.Value, &lessonTypeList)
				if err == nil {
					err = connector.lessonTypesListWriter.write(lessonTypeList)
				}
			}
		}

		if err == nil {
			err = connector.reader.CommitMessages(context.Background(), message)
			fmt.Fprintf(connector.out, "%T Commit %s message (err: %v) \n", connector, messageKey, err)
		}

		if err != nil && err != context.Canceled {
			fmt.Fprintf(connector.out, "%T error: %v \n", connector, err)
		}
	}

	wg.Done()
}
