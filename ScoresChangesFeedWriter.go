package main

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/kneu-messenger-pigeon/events"
	"github.com/segmentio/kafka-go"
	"io"
	"sync"
	"time"
)

type ScoresChangesFeedWriterInterface interface {
	execute(ctx context.Context)
	addToQueue(event events.ScoreEvent, previousValue events.ScoreValue)
}

type ScoresChangesFeedWriter struct {
	out             io.Writer
	writer          events.WriterInterface
	eventQueue      []events.ScoreChangedEvent
	eventQueueMutex sync.Mutex
}

func (writer *ScoresChangesFeedWriter) execute(ctx context.Context) {
	var err error
	for ctx.Err() == nil || len(writer.eventQueue) != 0 {
		if len(writer.eventQueue) != 0 {
			fmt.Fprintf(writer.out, "Write %d score changes into feed... \n", len(writer.eventQueue))
			err = writer.writeEvents()
			if err != nil {
				fmt.Fprintf(writer.out, "Failed to push score changes events: %s\n", err)
			}
		} else {
			time.Sleep(time.Millisecond)
		}
	}
}

func (writer *ScoresChangesFeedWriter) writeEvents() error {
	var payload []byte
	messages := make([]kafka.Message, len(writer.eventQueue))
	for i, message := range writer.eventQueue {
		payload, _ = json.Marshal(message)
		messages[i] = kafka.Message{
			Key:   message.GetMessageKey(),
			Value: payload,
		}
	}

	err := writer.writer.WriteMessages(context.Background(), messages...)
	if err == nil {
		writer.eventQueueMutex.Lock()
		writer.eventQueue = writer.eventQueue[len(messages):len(writer.eventQueue)]
		writer.eventQueueMutex.Unlock()
	}
	return err
}

func (writer *ScoresChangesFeedWriter) addToQueue(event events.ScoreEvent, previousValue events.ScoreValue) {
	changedEvent := events.ScoreChangedEvent{
		ScoreEvent: event,
		Previous:   previousValue,
	}

	writer.eventQueueMutex.Lock()
	writer.eventQueue = append(writer.eventQueue, changedEvent)
	writer.eventQueueMutex.Unlock()
}
