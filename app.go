package main

import (
	"context"
	"fmt"
	"github.com/go-redis/redis/v9"
	"github.com/kneu-messenger-pigeon/events"
	victoriaMetricsInit "github.com/kneu-messenger-pigeon/victoria-metrics-init"
	"github.com/segmentio/kafka-go"
	"io"
	"os"
	"time"
)

const ExitCodeMainError = 1

func runApp(out io.Writer) error {
	var opt *redis.Options

	envFilename := ""
	if _, err := os.Stat(".env"); err == nil {
		envFilename = ".env"
	}

	config, err := loadConfig(envFilename)
	if err == nil {
		opt, err = redis.ParseURL(config.redisDsn)
	}
	victoriaMetricsInit.InitMetrics("storage-writer")

	if err != nil {
		return err
	}

	redisClient := redis.NewClient(opt)
	groupId := "storage-writer"

	scoresChangesFeedWriter := NewScoresChangesFeedWriter(
		out,
		&kafka.Writer{
			Addr:     kafka.TCP(config.kafkaHost),
			Topic:    events.ScoresChangesFeedTopic,
			Balancer: &kafka.Murmur2Balancer{},
		},
		newLessonExistChecker(redisClient),
	)

	scoreWriter := &ScoreWriter{
		scoresChangesFeedWriter: scoresChangesFeedWriter,
	}

	scoreConnector1 := &KafkaToRedisConnector{
		out:    out,
		redis:  redisClient,
		writer: scoreWriter,
		reader: kafka.NewReader(
			kafka.ReaderConfig{
				Brokers:     []string{config.kafkaHost},
				GroupID:     groupId,
				Topic:       events.RawScoresTopic,
				MinBytes:    10,
				MaxBytes:    10e3,
				MaxWait:     time.Second,
				MaxAttempts: config.kafkaAttempts,
				Dialer: &kafka.Dialer{
					Timeout:   config.kafkaTimeout,
					DualStack: kafka.DefaultDialer.DualStack,
				},
			},
		),
	}

	scoreConnector2 := &KafkaToRedisConnector{
		out:    out,
		redis:  redisClient,
		writer: scoreWriter,
		reader: kafka.NewReader(
			kafka.ReaderConfig{
				Brokers:     []string{config.kafkaHost},
				GroupID:     groupId,
				Topic:       events.RawScoresTopic,
				MinBytes:    10,
				MaxBytes:    10e3,
				MaxWait:     time.Second,
				MaxAttempts: config.kafkaAttempts,
				Dialer: &kafka.Dialer{
					Timeout:   config.kafkaTimeout,
					DualStack: kafka.DefaultDialer.DualStack,
				},
			},
		),
	}

	lessonWriter := &LessonWriter{}

	lessonConnector1 := &KafkaToRedisConnector{
		out:    out,
		redis:  redisClient,
		writer: lessonWriter,
		reader: kafka.NewReader(
			kafka.ReaderConfig{
				Brokers:     []string{config.kafkaHost},
				GroupID:     groupId,
				Topic:       events.RawLessonsTopic,
				MinBytes:    10,
				MaxBytes:    10e3,
				MaxWait:     time.Second,
				MaxAttempts: config.kafkaAttempts,
				Dialer: &kafka.Dialer{
					Timeout:   config.kafkaTimeout,
					DualStack: kafka.DefaultDialer.DualStack,
				},
			},
		),
	}

	lessonConnector2 := &KafkaToRedisConnector{
		out:    out,
		redis:  redisClient,
		writer: lessonWriter,
		reader: kafka.NewReader(
			kafka.ReaderConfig{
				Brokers:     []string{config.kafkaHost},
				GroupID:     groupId,
				Topic:       events.RawLessonsTopic,
				MinBytes:    10,
				MaxBytes:    10e3,
				MaxWait:     time.Second,
				MaxAttempts: config.kafkaAttempts,
				Dialer: &kafka.Dialer{
					Timeout:   config.kafkaTimeout,
					DualStack: kafka.DefaultDialer.DualStack,
				},
			},
		),
	}

	disciplineConnector := &KafkaToRedisConnector{
		out:    out,
		redis:  redisClient,
		writer: &DisciplineWriter{},
		reader: kafka.NewReader(
			kafka.ReaderConfig{
				Brokers:     []string{config.kafkaHost},
				GroupID:     groupId,
				Topic:       events.DisciplinesTopic,
				MinBytes:    10,
				MaxBytes:    10e3,
				MaxWait:     time.Second,
				MaxAttempts: config.kafkaAttempts,
				Dialer: &kafka.Dialer{
					Timeout:   config.kafkaTimeout,
					DualStack: kafka.DefaultDialer.DualStack,
				},
			},
		),
	}

	metaEventsConnector := &KafkaToRedisMetaEventsConnector{
		out:   out,
		redis: redisClient,
		currentYearWriter: &YearChangeWriter{
			out:                  out,
			isValidEducationYear: isValidEducationYear,
		},
		lessonTypesListWriter: &LessonTypesListWriter{},
		reader: kafka.NewReader(
			kafka.ReaderConfig{
				Brokers:     []string{config.kafkaHost},
				GroupID:     groupId,
				Topic:       events.MetaEventsTopic,
				MinBytes:    10,
				MaxBytes:    10e3,
				MaxWait:     time.Second,
				MaxAttempts: config.kafkaAttempts,
				Dialer: &kafka.Dialer{
					Timeout:   config.kafkaTimeout,
					DualStack: kafka.DefaultDialer.DualStack,
				},
			},
		),
	}

	eventLoop := EventLoop{
		connectorsPool: [ConnectorPoolSize]ConnectorInterface{
			scoreConnector1,
			scoreConnector2,
			lessonConnector1,
			lessonConnector2,
			disciplineConnector,
			metaEventsConnector,
		},
		scoresChangesFeedWriter: scoresChangesFeedWriter,
	}

	defer func() {
		redisClient.BgSave(context.Background())
		_ = redisClient.Close()

		_ = scoreConnector1.reader.Close()
		_ = scoreConnector2.reader.Close()
		_ = lessonConnector1.reader.Close()
		_ = disciplineConnector.reader.Close()
	}()
	eventLoop.execute()
	return nil
}

func handleExitError(errStream io.Writer, err error) int {
	if err != nil {
		_, _ = fmt.Fprintln(errStream, err)
	}

	if err != nil {
		return ExitCodeMainError
	}

	return 0
}
