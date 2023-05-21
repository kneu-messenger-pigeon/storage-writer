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

		expectedDiscipline := events.Discipline{
			Id:   987,
			Name: "Право та беззаконня",
		}

		expectedEvent := events.ScoreChangedEvent{
			Discipline: expectedDiscipline,
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

		disciplineRepository := NewMockDisciplineRepositoryInterface(t)
		disciplineRepository.On("GetDiscipline", expectedEvent.Year, expectedEvent.DisciplineId).Return(expectedDiscipline)

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
			out:                  out,
			writer:               writer,
			disciplineRepository: disciplineRepository,
		}

		go scoresChangesFeedWriter.execute(ctx)
		scoresChangesFeedWriter.addToQueue(expectedEvent.ScoreEvent, expectedEvent.Previous)
		<-ctx.Done()
	})

	t.Run("writeFeed - error write to Kafka", func(t *testing.T) {
		expectedDiscipline := events.Discipline{
			Id:   987,
			Name: "Право та беззаконня",
		}

		expectedError := errors.New("expected error")
		expectedEvent := events.ScoreChangedEvent{
			Discipline: expectedDiscipline,
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

		disciplineRepository := NewMockDisciplineRepositoryInterface(t)
		disciplineRepository.On("GetDiscipline", expectedEvent.Year, expectedEvent.DisciplineId).Return(expectedDiscipline)

		out := &bytes.Buffer{}
		ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond*10)
		var savedQueue []events.ScoreChangedEvent

		writer := events.NewMockWriterInterface(t)
		scoresChangesFeedWriter := ScoresChangesFeedWriter{
			out:                  out,
			writer:               writer,
			disciplineRepository: disciplineRepository,
		}

		expectedEventMessage := func(message kafka.Message) bool {
			if ctx.Err() != nil {
				savedQueue = make([]events.ScoreChangedEvent, len(scoresChangesFeedWriter.eventQueue))
				copy(savedQueue, scoresChangesFeedWriter.eventQueue)
				scoresChangesFeedWriter.eventQueue = make([]events.ScoreChangedEvent, 0)
			}

			cancel()
			var actualEvent events.ScoreChangedEvent

			_ = json.Unmarshal(message.Value, &actualEvent)

			return assert.Equal(t, events.ScoreChangedEventName, string(message.Key)) &&
				assert.Equal(t, expectedEvent, actualEvent)
		}
		writer.On("WriteMessages", matchContext, mock.MatchedBy(expectedEventMessage)).Return(expectedError)

		scoresChangesFeedWriter.addToQueue(expectedEvent.ScoreEvent, expectedEvent.Previous)
		scoresChangesFeedWriter.execute(ctx)
		<-ctx.Done()

		assert.Equal(t, 1, len(savedQueue))
		assert.Equal(t, expectedEvent, savedQueue[0])
	})
}
