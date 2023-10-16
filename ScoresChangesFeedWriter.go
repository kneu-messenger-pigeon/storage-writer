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
	out                 io.Writer
	writer              events.WriterInterface
	readyQueue          eventQueueMutex
	waitingQueue        eventQueueMutex
	maxLessonId         MaxLessonIdGetterInterface
	lastCheckedLessonId uint
}

type eventQueueMutex struct {
	queue []*events.ScoreChangedEvent
	mutex sync.Mutex
}

func (writer *ScoresChangesFeedWriter) execute(ctx context.Context) {
	ticker := time.NewTicker(time.Millisecond * 100)

	continueLoop := true
	for continueLoop {
		select {
		case <-writer.maxLessonId.Changed():
		case <-ticker.C:
		case <-ctx.Done():
			continueLoop = false
		}

		writer.checkWaiting(!continueLoop)
		writer.writeEvents()
	}

	ticker.Stop()
}

func (writer *ScoresChangesFeedWriter) writeEvents() {
	if len(writer.readyQueue.queue) == 0 {
		return
	}

	fmt.Fprintf(writer.out, "Write %d score changes into scores changed feed... \n", len(writer.readyQueue.queue))

	var payload []byte
	queueLength := len(writer.readyQueue.queue)
	messages := make([]kafka.Message, queueLength)
	for i := 0; i < queueLength; i++ {
		payload, _ = json.Marshal(writer.readyQueue.queue[i])
		messages[i] = kafka.Message{
			Key:   writer.readyQueue.queue[i].GetMessageKey(),
			Value: payload,
		}
	}

	err := writer.writer.WriteMessages(context.Background(), messages...)
	if err == nil {
		writer.readyQueue.sliceLeft(queueLength)
	}

	if err != nil {
		fmt.Fprintf(writer.out, "Failed to push score changes events: %s\n", err)
	}
}

func (writer *ScoresChangesFeedWriter) checkWaiting(force bool) {
	if len(writer.waitingQueue.queue) == 0 {
		return
	}

	if writer.maxLessonId.Get() == writer.lastCheckedLessonId && !force {
		return
	}

	writer.lastCheckedLessonId = writer.maxLessonId.Get()
	syncAtDeadline := time.Now().Add(-10 * time.Minute)

	var event *events.ScoreChangedEvent
	queueLength := len(writer.waitingQueue.queue)

	writer.readyQueue.mutex.Lock()
	for i := 0; i < queueLength; i++ {
		event = writer.waitingQueue.queue[i]
		if event != nil {
			if event.LessonId <= writer.lastCheckedLessonId || event.SyncedAt.Before(syncAtDeadline) || force {
				writer.readyQueue.queue = append(writer.readyQueue.queue, event)
				writer.waitingQueue.queue[i] = nil
			}
		}
	}
	writer.readyQueue.mutex.Unlock()

	lastNullIndex := -1
	for i := 0; i < queueLength; i++ {
		if writer.waitingQueue.queue[i] == nil {
			lastNullIndex = i
		} else {
			break
		}
	}

	if lastNullIndex >= 0 {
		writer.waitingQueue.sliceLeft(lastNullIndex + 1)
	}
}

func (writer *ScoresChangesFeedWriter) addToQueue(event events.ScoreEvent, previousValue events.ScoreValue) {
	changedEvent := events.ScoreChangedEvent{
		ScoreEvent: event,
		Previous:   previousValue,
	}

	if changedEvent.LessonId > writer.maxLessonId.Get() {
		writer.waitingQueue.append(&changedEvent)
	} else {
		writer.readyQueue.append(&changedEvent)
	}

	if event.ScoreSource == events.Realtime {
		realtimeScoresChangesCount.Inc()
	} else if event.ScoreSource == events.Secondary {
		secondaryScoresChangesCount.Inc()
	}
}

func (queue *eventQueueMutex) append(changedEvent *events.ScoreChangedEvent) {
	queue.mutex.Lock()
	queue.queue = append(queue.queue, changedEvent)
	queue.mutex.Unlock()
}

func (queue *eventQueueMutex) sliceLeft(leftIndex int) {
	queue.mutex.Lock()
	queue.queue = queue.queue[leftIndex:len(queue.queue)]
	queue.mutex.Unlock()
}
