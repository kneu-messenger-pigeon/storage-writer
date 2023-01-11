package main

import (
	"context"
	"github.com/go-redis/redis/v9"
	"github.com/segmentio/kafka-go"
	"os"
	"time"
)

func main() {
	envFilename := ""
	if _, err := os.Stat(".env"); err == nil {
		envFilename = ".env"
	}

	config, err := loadConfig(envFilename)
	if err != nil {
		//		return errors.New("Failed to load config: " + err.Error())
	}

	opt, err := redis.ParseURL(config.redisDsn)
	if err != nil {
		panic(err)
	}

	redisClient := redis.NewClient(opt)
	groupId := "storage-writer"

	scoresChangesFeedWriter := &ScoresChangesFeedWriter{
		out: os.Stdout,
		writer: &kafka.Writer{
			Addr:     kafka.TCP(config.kafkaHost),
			Topic:    "scores_changes_feed",
			Balancer: &kafka.LeastBytes{},
		},
	}

	scoreConnector1 := &KafkaToRedisConnector{
		out:   os.Stdout,
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
		out:   os.Stdout,
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
		out:    os.Stdout,
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
		out:    os.Stdout,
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
		connectorsPool: [ConnectorPoolSize]*KafkaToRedisConnector{
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
		for _, connector := range eventLoop.connectorsPool {
			_ = connector.reader.Close()
		}
	}()
	eventLoop.execute()
}
