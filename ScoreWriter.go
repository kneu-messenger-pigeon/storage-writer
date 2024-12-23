package main

import (
	"context"
	"errors"
	"fmt"
	"github.com/kneu-messenger-pigeon/events"
	"github.com/redis/go-redis/v9"
	"strconv"
)

const IsAbsentScoreValue = float64(-999999)

const maxWriteRetries = 3

type ScoreWriter struct {
	redis                   redis.UniversalClient
	scoresChangesFeedWriter ScoresChangesFeedWriterInterface
}

func (writer *ScoreWriter) setRedis(redis redis.UniversalClient) {
	writer.redis = redis
}

func (writer *ScoreWriter) getExpectedMessageKey() string {
	return events.ScoreEventName
}

func (writer *ScoreWriter) getExpectedEventType() any {
	return &events.ScoreEvent{}
}

func (writer *ScoreWriter) write(s any) (err error) {
	event := s.(*events.ScoreEvent)

	studentDisciplineScoresKey := fmt.Sprintf("%d:%d:scores:%d:%d", event.Year, event.Semester, event.StudentId, event.DisciplineId)
	lessonKey := fmt.Sprintf("%d:%d", event.LessonId, event.LessonPart)

	disciplineTotalsKey := fmt.Sprintf("%d:%d:totals:%d", event.Year, event.Semester, event.DisciplineId)

	disciplineLastUpdateAtKey := fmt.Sprintf("%d:discipline_semester_updated_at:%d", event.Year, event.DisciplineId)
	disciplineLastUpdateNewValue := fmt.Sprintf("%d%d", event.Semester, event.UpdatedAt.Unix())

	studentDisciplinesKey := fmt.Sprintf("%d:%d:student_disciplines:%d", event.Year, event.Semester, event.StudentId)
	studentKey := strconv.Itoa(int(event.StudentId))

	hasChanges := false
	var previousValue events.ScoreValue
	ctx := context.Background()
	writeValueFunc := func(tx *redis.Tx) (err error) {
		disciplineLastUpdateStoredValue := writer.redis.Get(ctx, disciplineLastUpdateAtKey).Val()
		storedValue, err := writer.redis.HGet(ctx, studentDisciplineScoresKey, lessonKey).Float64()
		storedIsDeleted := errors.Is(err, redis.Nil)
		if err != nil && !storedIsDeleted {
			return err
		}
		newValue := makeScoreStorageValue(event)

		if event.IsDeleted == storedIsDeleted && newValue == storedValue {
			// do nothing, storage state equal to event (value match or already deleted form storage)
			return nil
		}
		_, err = tx.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
			if disciplineLastUpdateNewValue > disciplineLastUpdateStoredValue {
				pipe.Set(ctx, disciplineLastUpdateAtKey, disciplineLastUpdateNewValue, 0)
			}

			if event.IsDeleted {
				pipe.HDel(ctx, studentDisciplineScoresKey, lessonKey)
			} else {
				pipe.HSet(ctx, studentDisciplineScoresKey, lessonKey, newValue)
			}

			scoreDiff := float64(0)
			if storedValue != IsAbsentScoreValue {
				scoreDiff -= storedValue
			}
			if newValue != IsAbsentScoreValue {
				scoreDiff += newValue
			}
			if scoreDiff != 0 {
				pipe.ZIncrBy(ctx, disciplineTotalsKey, scoreDiff, studentKey)
			}
			return nil
		})
		if err == nil {
			hasChanges = true
			previousValue = events.ScoreValue{
				IsAbsent:  storedValue == IsAbsentScoreValue,
				IsDeleted: storedIsDeleted,
			}

			if storedValue != IsAbsentScoreValue {
				previousValue.Value = float32(storedValue)
			}
		}
		return err
	}

	// Retry if the key has been changed.
	for i := 0; i < maxWriteRetries; i++ {
		err = writer.redis.Watch(ctx, writeValueFunc, studentDisciplineScoresKey)
		if err == nil || !errors.Is(err, redis.TxFailedErr) {
			break
		}
	}

	if hasChanges && err == nil {
		var isMember bool
		isMember, err = writer.redis.SIsMember(ctx, studentDisciplinesKey, event.DisciplineId).Result()
		if !isMember {
			err = writer.redis.SAdd(ctx, studentDisciplinesKey, event.DisciplineId).Err()
		}

		writer.scoresChangesFeedWriter.addToQueue(*event, previousValue)
	}
	return err
}

func makeScoreStorageValue(event *events.ScoreEvent) float64 {
	if event.IsDeleted {
		return 0
	} else if event.IsAbsent {
		return IsAbsentScoreValue
	} else {
		return float64(event.Value)
	}
}
