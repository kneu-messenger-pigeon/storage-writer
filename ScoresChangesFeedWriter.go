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
	tick := time.Tick(time.Millisecond * 10)

	continueLoop := true
	for continueLoop {
		select {
		case <-tick:
		case <-ctx.Done():
			continueLoop = false
		}

		writer.writeEvents()
	}
}

func (writer *ScoresChangesFeedWriter) writeEvents() {
	if len(writer.eventQueue) == 0 {
		return
	}

	fmt.Fprintf(writer.out, "Write %d score changes into scores changed feed... \n", len(writer.eventQueue))

	var payload []byte
	queueLength := len(writer.eventQueue)
	messages := make([]kafka.Message, queueLength)
	for i := 0; i < queueLength; i++ {
		payload, _ = json.Marshal(writer.eventQueue[i])
		messages[i] = kafka.Message{
			Key:   writer.eventQueue[i].GetMessageKey(),
			Value: payload,
		}
	}

	err := writer.writer.WriteMessages(context.Background(), messages...)
	if err == nil {
		writer.eventQueueMutex.Lock()
		writer.eventQueue = writer.eventQueue[queueLength:len(writer.eventQueue)]
		writer.eventQueueMutex.Unlock()
	}

	if err != nil {
		fmt.Fprintf(writer.out, "Failed to push score changes events: %s\n", err)
	}
}

func (writer *ScoresChangesFeedWriter) addToQueue(event events.ScoreEvent, previousValue events.ScoreValue) {
	changedEvent := events.ScoreChangedEvent{
		ScoreEvent: event,
		Previous:   previousValue,
	}

	writer.eventQueueMutex.Lock()
	writer.eventQueue = append(writer.eventQueue, changedEvent)
	writer.eventQueueMutex.Unlock()

	if event.ScoreSource == events.Realtime {
		realtimeScoresChangesCount.Inc()
	} else if event.ScoreSource == events.Secondary {
		secondaryScoresChangesCount.Inc()
	}
}
