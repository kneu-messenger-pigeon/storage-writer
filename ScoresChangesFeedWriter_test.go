package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"github.com/kneu-messenger-pigeon/events"
	"github.com/segmentio/kafka-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"testing"
	"time"
)

func TestScoresChangesFeedWriter(t *testing.T) {
	matchContext := mock.MatchedBy(func(ctx context.Context) bool { return true })

	t.Run("writeFeed", func(t *testing.T) {
		out := &bytes.Buffer{}

		expectedEvent := events.ScoreChangedEvent{
			Id:           112233,
			StudentId:    123,
			LessonId:     150,
			LessonPart:   1,
			DisciplineId: 234,
			Year:         2028,
			Semester:     1,
			Value:        2.5,
			IsAbsent:     false,
			IsDeleted:    false,
			UpdatedAt:    time.Date(2028, time.Month(11), 18, 14, 30, 40, 0, time.Local),
			SyncedAt:     time.Date(2028, time.Month(11), 18, 14, 35, 13, 0, time.Local),
		}

		ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond*10)

		expectedEventMessage := func(message kafka.Message) bool {
			cancel()
			var actualEvent events.ScoreChangedEvent

			_ = json.Unmarshal(message.Value, &actualEvent)

			return assert.Equal(t, events.ScoreChangedEventName, string(message.Key)) &&
				assert.Equal(t, expectedEvent, actualEvent)
		}

		writer := events.NewMockWriterInterface(t)
		writer.On("WriteMessages", matchContext, mock.MatchedBy(expectedEventMessage)).Return(nil)

		scoresChangesFeedWriter := ScoresChangesFeedWriter{
			out:    out,
			writer: writer,
		}

		scoreEvent := scoreChangedEventToScoreEvent(expectedEvent)

		go scoresChangesFeedWriter.execute(ctx)
		scoresChangesFeedWriter.addToQueue(scoreEvent)
		<-ctx.Done()
	})

	t.Run("writeFeed - error write to Kafka", func(t *testing.T) {
		expectedError := errors.New("expected error")
		expectedEvent := events.ScoreChangedEvent{
			Id:           112233,
			StudentId:    123,
			LessonId:     150,
			LessonPart:   1,
			DisciplineId: 234,
			Year:         2028,
			Semester:     1,
			Value:        2.5,
			IsAbsent:     false,
			IsDeleted:    false,
			UpdatedAt:    time.Date(2028, time.Month(11), 18, 14, 30, 40, 0, time.Local),
			SyncedAt:     time.Date(2028, time.Month(11), 18, 14, 35, 13, 0, time.Local),
		}

		out := &bytes.Buffer{}
		ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond*10)
		var savedQueue []events.ScoreEvent

		writer := events.NewMockWriterInterface(t)
		scoresChangesFeedWriter := ScoresChangesFeedWriter{
			out:    out,
			writer: writer,
		}

		expectedEventMessage := func(message kafka.Message) bool {
			if ctx.Err() != nil {
				savedQueue = scoresChangesFeedWriter.eventQueue
				scoresChangesFeedWriter.eventQueue = make([]events.ScoreEvent, 0)
			}

			cancel()
			var actualEvent events.ScoreChangedEvent

			_ = json.Unmarshal(message.Value, &actualEvent)

			return assert.Equal(t, events.ScoreChangedEventName, string(message.Key)) &&
				assert.Equal(t, expectedEvent, actualEvent)
		}
		writer.On("WriteMessages", matchContext, mock.MatchedBy(expectedEventMessage)).Return(expectedError)

		scoreEvent := scoreChangedEventToScoreEvent(expectedEvent)

		scoresChangesFeedWriter.addToQueue(scoreEvent)
		scoresChangesFeedWriter.execute(ctx)
		<-ctx.Done()

		assert.Equal(t, 1, len(savedQueue))
		assert.Equal(t, scoreEvent, savedQueue[0])
	})
}

func scoreChangedEventToScoreEvent(scoreChangedEvent events.ScoreChangedEvent) events.ScoreEvent {
	return events.ScoreEvent{
		Id:           scoreChangedEvent.Id,
		StudentId:    scoreChangedEvent.StudentId,
		LessonId:     scoreChangedEvent.LessonId,
		LessonPart:   scoreChangedEvent.LessonPart,
		DisciplineId: scoreChangedEvent.DisciplineId,
		Year:         scoreChangedEvent.Year,
		Semester:     scoreChangedEvent.Semester,
		Value:        scoreChangedEvent.Value,
		IsAbsent:     scoreChangedEvent.IsAbsent,
		IsDeleted:    scoreChangedEvent.IsDeleted,
		UpdatedAt:    scoreChangedEvent.UpdatedAt,
		SyncedAt:     scoreChangedEvent.SyncedAt,
	}

}
