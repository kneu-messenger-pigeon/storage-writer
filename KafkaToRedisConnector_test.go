package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/go-redis/redismock/v9"
	"github.com/kneu-messenger-pigeon/events"
	"github.com/kneu-messenger-pigeon/events/mocks"
	"github.com/segmentio/kafka-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

func TestKafkaToRedisConnector(t *testing.T) {
	matchContext := mock.MatchedBy(func(ctx context.Context) bool { return true })

	t.Run("Two iteration and commit", func(t *testing.T) {
		out := &bytes.Buffer{}

		event := events.DisciplineEvent{
			Year: 2035,
			Discipline: events.Discipline{
				Id:   1,
				Name: "test discipline name",
			},
		}

		payload, _ := json.Marshal(event)
		message := kafka.Message{
			Key:   []byte(events.DisciplineEventName),
			Value: payload,
		}

		ctx, cancel := context.WithCancel(context.Background())

		redis, redisMock := redismock.NewClientMock()

		writer := NewMockWriterInterface(t)
		writer.On("setRedis", redis).Once()
		writer.On("write", &event).Once().Return(nil)
		writer.On("getExpectedMessageKey").Return(events.DisciplineEventName)
		writer.On("getExpectedEventType").Return(&events.DisciplineEvent{})

		reader := mocks.NewReaderInterface(t)

		reader.On("FetchMessage", matchContext).Once().Return(message, nil)
		reader.On("FetchMessage", matchContext).Once().Return(func(_ context.Context) kafka.Message {
			cancel()
			return kafka.Message{}
		}, nil)

		reader.On("CommitMessages", matchContext, message, kafka.Message{}).Return(nil)

		connector := KafkaToRedisConnector{
			out:    out,
			redis:  redis,
			reader: reader,
			writer: writer,
		}

		wg := sync.WaitGroup{}
		wg.Add(1)
		connector.execute(ctx, &wg)

		assert.NoError(t, redisMock.ExpectationsWereMet())
	})

	t.Run("Emulate write error", func(t *testing.T) {
		expectedError := errors.New("expected error")

		out := &bytes.Buffer{}

		event := events.DisciplineEvent{
			Year: 2035,
			Discipline: events.Discipline{
				Id:   1,
				Name: "test discipline name",
			},
		}

		payload, _ := json.Marshal(event)
		message := kafka.Message{
			Key:   []byte(events.DisciplineEventName),
			Value: payload,
		}

		ctx, cancel := context.WithCancel(context.Background())

		redis, redisMock := redismock.NewClientMock()

		writer := NewMockWriterInterface(t)
		writer.On("setRedis", redis).Once()
		writer.On("write", &event).Once().Return(expectedError)
		writer.On("getExpectedMessageKey").Return(events.DisciplineEventName)
		writer.On("getExpectedEventType").Return(&events.DisciplineEvent{})

		reader := mocks.NewReaderInterface(t)

		reader.On("FetchMessage", matchContext).Once().Return(func(_ context.Context) kafka.Message {
			cancel()
			return message
		}, nil)

		connector := KafkaToRedisConnector{
			out:    out,
			redis:  redis,
			reader: reader,
			writer: writer,
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

		redis, redisMock := redismock.NewClientMock()

		writer := NewMockWriterInterface(t)
		writer.On("setRedis", redis).Once()
		writer.On("getExpectedMessageKey").Return("")
		writer.On("getExpectedEventType").Return(&events.DisciplineEvent{}).Maybe()

		reader := mocks.NewReaderInterface(t)

		connector := KafkaToRedisConnector{
			out:    out,
			redis:  redis,
			reader: reader,
			writer: writer,
		}

		wg := sync.WaitGroup{}
		wg.Add(1)

		ctx, _ := context.WithTimeout(context.Background(), time.Millisecond)
		connector.execute(ctx, &wg)

		assert.NoError(t, redisMock.ExpectationsWereMet())
		reader.AssertNotCalled(t, "FetchMessage")
		reader.AssertNotCalled(t, "CommitMessages")
		writer.AssertNotCalled(t, "write")

		assert.Empty(t, out.String())
	})
}
