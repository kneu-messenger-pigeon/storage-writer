package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"github.com/go-redis/redismock/v9"
	"github.com/kneu-messenger-pigeon/events"
	"github.com/kneu-messenger-pigeon/events/mocks"
	"github.com/segmentio/kafka-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"runtime"
	"testing"
	"time"
)

func TestScoresChangesFeedWriterAddToQueue(t *testing.T) {
	t.Run("sourceRealtime", func(t *testing.T) {
		scoreEvent := events.ScoreEvent{
			ScoreSource: events.Realtime,
		}

		scoresChangesFeedWriter := ScoresChangesFeedWriter{
			maxLessonId: &MaxLessonId{},
		}
		scoresChangesFeedWriter.addToQueue(scoreEvent, events.ScoreValue{})

		assert.Equal(t, 1, len(scoresChangesFeedWriter.readyQueue.queue))
		assert.Equal(t, uint64(1), realtimeScoresChangesCount.Get())
	})

	t.Run("sourceSecondary", func(t *testing.T) {
		scoreEvent := events.ScoreEvent{
			ScoreSource: events.Secondary,
		}

		scoresChangesFeedWriter := ScoresChangesFeedWriter{
			maxLessonId: &MaxLessonId{},
		}
		scoresChangesFeedWriter.addToQueue(scoreEvent, events.ScoreValue{})

		assert.Equal(t, 1, len(scoresChangesFeedWriter.readyQueue.queue))
		assert.Equal(t, uint64(1), secondaryScoresChangesCount.Get())
	})
}

func TestScoresChangesFeedWriter(t *testing.T) {
	matchContext := mock.MatchedBy(func(ctx context.Context) bool { return true })

	testWriteFeed := func(t *testing.T, withWaiting bool) {
		out := &bytes.Buffer{}

		expectedEvent := events.ScoreChangedEvent{
			ScoreEvent: events.ScoreEvent{
				Id:           112233,
				StudentId:    123,
				LessonId:     150,
				LessonPart:   1,
				DisciplineId: 234,
				Year:         2028,
				Semester:     1,
				ScoreSource:  events.Realtime,
				ScoreValue: events.ScoreValue{
					Value:     2.5,
					IsAbsent:  false,
					IsDeleted: false,
				},
				UpdatedAt: time.Date(2028, time.Month(11), 18, 14, 30, 40, 0, time.Local),
				SyncedAt:  time.Date(2028, time.Month(11), 18, 14, 35, 13, 0, time.Local),
			},
			Previous: events.ScoreValue{
				Value:     0,
				IsAbsent:  false,
				IsDeleted: true,
			},
		}

		ctx, cancel := context.WithCancel(context.Background())

		receivedEvents := make(chan events.ScoreChangedEvent, 5)

		expectedEventMessage := func(message kafka.Message) bool {
			assert.Equal(t, events.ScoreChangedEventName, events.GetEventName(message.Key))

			var actualEvent events.ScoreChangedEvent
			_ = json.Unmarshal(message.Value, &actualEvent)
			receivedEvents <- actualEvent
			return true
		}

		writer := mocks.NewWriterInterface(t)
		writer.On("WriteMessages", matchContext, mock.MatchedBy(expectedEventMessage)).Return(nil)

		redis, _ := redismock.NewClientMock()
		maxLessonId := newMaxLessonIdStorage(redis)

		if withWaiting {
			maxLessonId.Set(100)
		} else {
			maxLessonId.Set(400)
		}

		scoresChangesFeedWriter := ScoresChangesFeedWriter{
			out:         out,
			writer:      writer,
			maxLessonId: maxLessonId,
		}

		go scoresChangesFeedWriter.execute(ctx)
		runtime.Gosched()
		time.Sleep(time.Millisecond * 3)

		scoresChangesFeedWriter.addToQueue(expectedEvent.ScoreEvent, expectedEvent.Previous)

		// should not be written to Kafka
		notExistsLessonScoreEvent := events.ScoreEvent{
			LessonId: 112233,
			SyncedAt: time.Unix(time.Now().Unix(), 0),
		}

		if withWaiting {
			scoresChangesFeedWriter.addToQueue(notExistsLessonScoreEvent, events.ScoreValue{})
			maxLessonId.Set(300)
		}

		runtime.Gosched()
		time.Sleep(time.Millisecond * 100)
		if withWaiting {
			assert.Equal(t, 1, len(scoresChangesFeedWriter.waitingQueue.queue))
		} else {
			assert.Empty(t, len(scoresChangesFeedWriter.waitingQueue.queue))
		}
		assert.Empty(t, len(scoresChangesFeedWriter.readyQueue.queue))

		writer.AssertNumberOfCalls(t, "WriteMessages", 1)

		receiveTimeout, receiveTimeoutCancel := context.WithTimeout(context.Background(), time.Second)
		defer receiveTimeoutCancel()
		var firstReceivedEvent events.ScoreChangedEvent
		select {
		case <-receiveTimeout.Done():
			t.Fatal("timeout")
		case firstReceivedEvent = <-receivedEvents:
		}

		assert.Equal(t, expectedEvent, firstReceivedEvent)

		cancel()
		runtime.Gosched()

		if withWaiting {
			var secondReceivedEvent events.ScoreChangedEvent
			select {
			case <-receiveTimeout.Done():
				t.Fatal("timeout")
			case secondReceivedEvent = <-receivedEvents:
			}
			assert.Equal(t, notExistsLessonScoreEvent, secondReceivedEvent.ScoreEvent)
		}

		runtime.Gosched()
		time.Sleep(time.Millisecond * 100)
	}

	t.Run("writeFeed-without-waiting", func(t *testing.T) {
		testWriteFeed(t, false)
	})

	t.Run("writeFeed-with-waiting", func(t *testing.T) {
		testWriteFeed(t, true)
	})

	t.Run("writeFeed - error write to Kafka", func(t *testing.T) {
		expectedError := errors.New("expected error")
		expectedEvent := events.ScoreChangedEvent{
			ScoreEvent: events.ScoreEvent{
				Id:           112233,
				StudentId:    123,
				LessonId:     150,
				LessonPart:   1,
				DisciplineId: 234,
				Year:         2028,
				Semester:     1,
				ScoreValue: events.ScoreValue{
					Value:     2.5,
					IsAbsent:  false,
					IsDeleted: false,
				},
				UpdatedAt: time.Date(2028, time.Month(11), 18, 14, 30, 40, 0, time.Local),
				SyncedAt:  time.Date(2028, time.Month(11), 18, 14, 35, 13, 0, time.Local),
			},
			Previous: events.ScoreValue{
				Value:     0,
				IsAbsent:  false,
				IsDeleted: true,
			},
		}

		out := &bytes.Buffer{}
		ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond*10)
		var savedQueue []*events.ScoreChangedEvent

		writer := mocks.NewWriterInterface(t)
		scoresChangesFeedWriter := ScoresChangesFeedWriter{
			out:    out,
			writer: writer,
			maxLessonId: &MaxLessonId{
				maxLessonId: 300,
			},
		}

		expectedEventMessage := func(message kafka.Message) bool {
			if ctx.Err() != nil {
				savedQueue = make([]*events.ScoreChangedEvent, len(scoresChangesFeedWriter.readyQueue.queue))
				copy(savedQueue, scoresChangesFeedWriter.readyQueue.queue)
				scoresChangesFeedWriter.readyQueue.queue = make([]*events.ScoreChangedEvent, 0)
			}

			cancel()
			var actualEvent events.ScoreChangedEvent

			_ = json.Unmarshal(message.Value, &actualEvent)

			return assert.Equal(t, events.ScoreChangedEventName, events.GetEventName(actualEvent.GetMessageKey())) &&
				assert.Equal(t, expectedEvent, actualEvent)
		}
		writer.On("WriteMessages", matchContext, mock.MatchedBy(expectedEventMessage)).Return(expectedError)

		scoresChangesFeedWriter.addToQueue(expectedEvent.ScoreEvent, expectedEvent.Previous)
		scoresChangesFeedWriter.execute(ctx)
		<-ctx.Done()

		assert.Equal(t, 1, len(savedQueue))
		assert.Equal(t, expectedEvent, *savedQueue[0])
	})
}
