package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/kneu-messenger-pigeon/events"
	"github.com/redis/go-redis/v9"
	"github.com/segmentio/kafka-go"
)

type ConnectorInterface interface {
	execute(ctx context.Context, wg *sync.WaitGroup)
}

type KafkaToRedisConnector struct {
	out    io.Writer
	reader events.ReaderInterface
	redis  redis.UniversalClient
	writer WriterInterface
}

const RedisBackgroundSaveInProgress = "ERR Background save already in progress"

func (connector *KafkaToRedisConnector) execute(ctx context.Context, wg *sync.WaitGroup) {
	var err error
	var message kafka.Message
	var messagesToCommit []kafka.Message
	var fetchContext = ctx
	var fetchContextCancel = func() {}

	connector.writer.setRedis(connector.redis)
	expectedMessageKey := connector.writer.getExpectedMessageKey()

	if expectedMessageKey == "" || connector.writer.getExpectedEventType() == nil {
		wg.Done()
		return
	}

	fmt.Fprintf(connector.out, "%T connector started \n", connector.writer)

	for ctx.Err() == nil {
		message, err = connector.reader.FetchMessage(fetchContext)
		if err == nil && expectedMessageKey == string(message.Key) {
			event := connector.writer.getExpectedEventType()
			err = json.Unmarshal(message.Value, &event)
			if err == nil {
				err = connector.writer.write(event)
			}
		}
		if err == nil {
			if len(messagesToCommit) == 0 {
				fetchContext, fetchContextCancel = context.WithTimeout(ctx, time.Second*60)
			}

			messagesToCommit = append(messagesToCommit, message)
		}

		if len(messagesToCommit) != 0 && (len(messagesToCommit) >= 5000 || fetchContext.Err() != nil) {
			// revert context with time to usual
			fetchContextCancel()
			fetchContext = ctx
			err = connector.reader.CommitMessages(context.Background(), messagesToCommit...)
			fmt.Fprintf(connector.out, "%T Commit %d messages (err: %v) \n", connector.writer, len(messagesToCommit), err)
			if err == nil {
				messagesToCommit = []kafka.Message{}
			}
		}

		if err != nil && !errors.Is(err, context.Canceled) {
			fmt.Fprintf(connector.out, "%T error: %v \n", connector.writer, err)
		}
	}
	fetchContextCancel()

	wg.Done()
}
