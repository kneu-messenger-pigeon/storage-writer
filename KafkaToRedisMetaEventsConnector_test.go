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
	"sync"
	"testing"
	"time"
)

func TestKafkaToRedisMetaEventsConnector(t *testing.T) {
	matchContext := mock.MatchedBy(func(ctx context.Context) bool { return true })

	t.Run("One iteration with CurrentYearEventName and commit", func(t *testing.T) {
		out := &bytes.Buffer{}
		event := events.CurrentYearEvent{
			Year: 2035,
		}

		payload, _ := json.Marshal(event)
		message := kafka.Message{
			Key:   []byte(events.CurrentYearEventName),
			Value: payload,
		}

		ctx, cancel := context.WithCancel(context.Background())
		redis, redisMock := redismock.NewClientMock()

		currentYearWriter := NewMockWriterInterface(t)
		currentYearWriter.On("setRedis", redis).Once()
		currentYearWriter.On("write", &event).Once().Return(nil)

		lessonTypesListWriter := NewMockWriterInterface(t)
		lessonTypesListWriter.On("setRedis", redis).Once()

		reader := mocks.NewReaderInterface(t)
		reader.On("FetchMessage", matchContext).Once().Return(func(_ context.Context) kafka.Message {
			cancel()
			return message
		}, nil)
		reader.On("CommitMessages", matchContext, message).Return(nil)

		connector := KafkaToRedisMetaEventsConnector{
			out:                   out,
			redis:                 redis,
			reader:                reader,
			currentYearWriter:     currentYearWriter,
			lessonTypesListWriter: lessonTypesListWriter,
		}

		wg := sync.WaitGroup{}
		wg.Add(1)
		connector.execute(ctx, &wg)

		assert.NoError(t, redisMock.ExpectationsWereMet())
	})

	t.Run("One iteration with LessonTypesList and commit", func(t *testing.T) {
		out := &bytes.Buffer{}
		event := events.LessonTypesList{
			Year: 2035,
			List: []events.LessonType{
				{
					Id:        20,
					ShortName: "Лек",
					LongName:  "Лекція",
				},
			},
		}
		payload, _ := json.Marshal(event)
		message := kafka.Message{
			Key:   []byte(events.LessonTypesListName),
			Value: payload,
		}

		ctx, cancel := context.WithCancel(context.Background())
		redis, redisMock := redismock.NewClientMock()

		currentYearWriter := NewMockWriterInterface(t)
		currentYearWriter.On("setRedis", redis).Once()

		lessonTypesListWriter := NewMockWriterInterface(t)
		lessonTypesListWriter.On("setRedis", redis).Once()
		lessonTypesListWriter.On("write", &event).Once().Return(nil)

		reader := mocks.NewReaderInterface(t)
		reader.On("FetchMessage", matchContext).Once().Return(func(_ context.Context) kafka.Message {
			cancel()
			return message
		}, nil)
		reader.On("CommitMessages", matchContext, message).Return(nil)

		connector := KafkaToRedisMetaEventsConnector{
			out:                   out,
			redis:                 redis,
			reader:                reader,
			currentYearWriter:     currentYearWriter,
			lessonTypesListWriter: lessonTypesListWriter,
		}

		wg := sync.WaitGroup{}
		wg.Add(1)
		connector.execute(ctx, &wg)

		assert.NoError(t, redisMock.ExpectationsWereMet())
	})

	t.Run("Emulate write error", func(t *testing.T) {
		expectedError := errors.New("expected error")

		out := &bytes.Buffer{}

		event := events.CurrentYearEvent{
			Year: 2035,
		}

		payload, _ := json.Marshal(event)
		message := kafka.Message{
			Key:   []byte(events.CurrentYearEventName),
			Value: payload,
		}

		ctx, cancel := context.WithCancel(context.Background())

		redis, redisMock := redismock.NewClientMock()

		writer := NewMockWriterInterface(t)
		writer.On("setRedis", redis).Times(2)
		writer.On("write", &event).Once().Return(expectedError)

		reader := mocks.NewReaderInterface(t)
		reader.On("FetchMessage", matchContext).Once().Return(func(_ context.Context) kafka.Message {
			cancel()
			return message
		}, nil)

		connector := KafkaToRedisMetaEventsConnector{
			out:                   out,
			redis:                 redis,
			reader:                reader,
			currentYearWriter:     writer,
			lessonTypesListWriter: writer,
		}

		wg := sync.WaitGroup{}
		wg.Add(1)
		connector.execute(ctx, &wg)

		assert.NoError(t, redisMock.ExpectationsWereMet())
		reader.AssertNotCalled(t, "CommitMessages")

		assert.Contains(t, out.String(), expectedError.Error())
	})

	t.Run("Emulate writer init error", func(t *testing.T) {
		out := &bytes.Buffer{}

		connector := KafkaToRedisMetaEventsConnector{
			out: out,
		}

		wg := sync.WaitGroup{}
		wg.Add(1)

		ctx, _ := context.WithTimeout(context.Background(), time.Millisecond)
		connector.execute(ctx, &wg)

		assert.Empty(t, out.String())
	})
}
