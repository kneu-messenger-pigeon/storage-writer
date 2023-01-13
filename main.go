package main

import (
	"context"
	"fmt"
	"github.com/go-redis/redis/v9"
	"github.com/segmentio/kafka-go"
	"io"
	"os"
	"time"
)

const ExitCodeMainError = 1

func main() {
	os.Exit(handleExitError(os.Stderr, runApp(os.Stdout)))
}

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

	if err != nil {
		return err
	}

	redisClient := redis.NewClient(opt)
	groupId := "storage-writer"

	scoresChangesFeedWriter := &ScoresChangesFeedWriter{
		out: out,
		writer: &kafka.Writer{
			Addr:     kafka.TCP(config.kafkaHost),
			Topic:    "scores_changes_feed",
			Balancer: &kafka.LeastBytes{},
		},
	}

	scoreConnector1 := &KafkaToRedisConnector{
		out:   out,
		redis: redisClient,
		writer: &ScoreWriter{
			scoresChangesFeedWriter: scoresChangesFeedWriter,
		},
		reader: kafka.NewReader(
			kafka.ReaderConfig{
				Brokers:     []string{config.kafkaHost},
				GroupID:     groupId,
				Topic:       "raw_scores",
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
		out:   out,
		redis: redisClient,
		writer: &ScoreWriter{
			scoresChangesFeedWriter: scoresChangesFeedWriter,
		},
		reader: kafka.NewReader(
			kafka.ReaderConfig{
				Brokers:     []string{config.kafkaHost},
				GroupID:     groupId,
				Topic:       "raw_scores",
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

	lessonConnector := &KafkaToRedisConnector{
		out:    out,
		redis:  redisClient,
		writer: &LessonWriter{},
		reader: kafka.NewReader(
			kafka.ReaderConfig{
				Brokers:     []string{config.kafkaHost},
				GroupID:     groupId,
				Topic:       "raw_lessons",
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
				Topic:       "disciplines",
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
			lessonConnector,
			disciplineConnector,
		},
		scoresChangesFeedWriter: scoresChangesFeedWriter,
	}

	defer func() {
		redisClient.BgSave(context.Background())
		_ = redisClient.Close()

		_ = scoreConnector1.reader.Close()
		_ = scoreConnector2.reader.Close()
		_ = lessonConnector.reader.Close()
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
