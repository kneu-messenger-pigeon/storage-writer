package main

import (
	"errors"
	"github.com/go-redis/redismock/v9"
	"github.com/kneu-messenger-pigeon/events"
	"github.com/stretchr/testify/assert"
	"strconv"
	"testing"
	"time"
)

func TestWriteScore(t *testing.T) {
	t.Run("general expectation score", func(t *testing.T) {
		scoreWriter := ScoreWriter{}

		assert.IsType(t, scoreWriter.getExpectedEventType(), &events.ScoreEvent{})
		assert.Equal(t, scoreWriter.getExpectedMessageKey(), events.ScoreEventName)
	})

	t.Run("write score", func(t *testing.T) {
		event := events.ScoreEvent{
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
			UpdatedAt:    time.Date(2028, time.Month(11), 12, 14, 30, 40, 0, time.Local),
			SyncedAt:     time.Date(2028, time.Month(11), 12, 14, 35, 13, 0, time.Local),
		}

		redis, redisMock := redismock.NewClientMock()

		studentDisciplineScoresKey := "2028:1:scores:123:234"
		lessonKey := "150:1"

		disciplineTotalsKey := "2028:1:totals:234"
		studentDisciplinesKey := "2028:1:student_disciplines:123"

		redisMock.MatchExpectationsInOrder(true)

		redisMock.ExpectWatch(studentDisciplineScoresKey)

		redisMock.ExpectHGet(studentDisciplineScoresKey, lessonKey).RedisNil()
		redisMock.ExpectTxPipeline()
		redisMock.ExpectHSet(studentDisciplineScoresKey, lessonKey, 2.5).SetVal(1)

		redisMock.ExpectZIncrBy(disciplineTotalsKey, 2.5, "123").SetVal(1)

		redisMock.ExpectTxPipelineExec()

		redisMock.ExpectSIsMember(studentDisciplinesKey, uint(234)).SetVal(false)
		redisMock.ExpectSAdd(studentDisciplinesKey, uint(234)).SetVal(1)

		scoresChangesFeedWriter := NewMockScoresChangesFeedWriterInterface(t)
		scoresChangesFeedWriter.On("addToQueue", event, scoreValue{
			Value:     0,
			IsAbsent:  false,
			IsDeleted: true,
		})

		scoreWriter := ScoreWriter{
			scoresChangesFeedWriter: scoresChangesFeedWriter,
		}
		scoreWriter.setRedis(redis)

		err := scoreWriter.write(&event)

		assert.NoError(t, err)
		assert.NoError(t, redisMock.ExpectationsWereMet())
	})

	t.Run("write absent", func(t *testing.T) {
		event := events.ScoreEvent{
			Id:           112233,
			StudentId:    123,
			LessonId:     150,
			LessonPart:   1,
			DisciplineId: 234,
			Year:         2028,
			Semester:     1,
			Value:        0,
			IsAbsent:     true,
			IsDeleted:    false,
			UpdatedAt:    time.Date(2028, time.Month(11), 12, 14, 30, 40, 0, time.Local),
			SyncedAt:     time.Date(2028, time.Month(11), 12, 14, 35, 13, 0, time.Local),
		}

		redis, redisMock := redismock.NewClientMock()

		studentDisciplineScoresKey := "2028:1:scores:123:234"
		lessonKey := "150:1"

		studentDisciplinesKey := "2028:1:student_disciplines:123"

		redisMock.MatchExpectationsInOrder(true)

		redisMock.ExpectWatch(studentDisciplineScoresKey)

		redisMock.ExpectHGet(studentDisciplineScoresKey, lessonKey).RedisNil()
		redisMock.ExpectTxPipeline()
		redisMock.ExpectHSet(studentDisciplineScoresKey, lessonKey, IsAbsentScoreValue).SetVal(1)

		redisMock.ExpectTxPipelineExec()

		redisMock.ExpectSIsMember(studentDisciplinesKey, uint(234)).SetVal(false)
		redisMock.ExpectSAdd(studentDisciplinesKey, uint(234)).SetVal(1)

		scoresChangesFeedWriter := NewMockScoresChangesFeedWriterInterface(t)
		scoresChangesFeedWriter.On("addToQueue", event, scoreValue{
			Value:     0,
			IsAbsent:  false,
			IsDeleted: true,
		})

		scoreWriter := ScoreWriter{
			scoresChangesFeedWriter: scoresChangesFeedWriter,
		}
		scoreWriter.setRedis(redis)

		err := scoreWriter.write(&event)

		assert.IsType(t, scoreWriter.getExpectedEventType(), &event)
		assert.Equal(t, scoreWriter.getExpectedMessageKey(), events.ScoreEventName)

		assert.NoError(t, err)
		assert.NoError(t, redisMock.ExpectationsWereMet())
	})

	t.Run("write not changed score", func(t *testing.T) {
		event := events.ScoreEvent{
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
			UpdatedAt:    time.Date(2028, time.Month(11), 12, 14, 30, 40, 0, time.Local),
			SyncedAt:     time.Date(2028, time.Month(11), 12, 14, 35, 13, 0, time.Local),
		}

		redis, redisMock := redismock.NewClientMock()

		studentDisciplineScoresKey := "2028:1:scores:123:234"
		lessonKey := "150:1"

		redisMock.MatchExpectationsInOrder(true)

		redisMock.ExpectWatch(studentDisciplineScoresKey)
		redisMock.ExpectHGet(studentDisciplineScoresKey, lessonKey).SetVal("2.5")

		scoresChangesFeedWriter := NewMockScoresChangesFeedWriterInterface(t)

		scoreWriter := ScoreWriter{
			scoresChangesFeedWriter: scoresChangesFeedWriter,
		}
		scoreWriter.setRedis(redis)

		err := scoreWriter.write(&event)

		assert.IsType(t, scoreWriter.getExpectedEventType(), &event)
		assert.Equal(t, scoreWriter.getExpectedMessageKey(), events.ScoreEventName)

		assert.NoError(t, err)
		assert.NoError(t, redisMock.ExpectationsWereMet())

		scoresChangesFeedWriter.AssertNotCalled(t, "addToQueue")
	})

	t.Run("write not changed absent", func(t *testing.T) {
		event := events.ScoreEvent{
			Id:           112233,
			StudentId:    123,
			LessonId:     150,
			LessonPart:   1,
			DisciplineId: 234,
			Year:         2028,
			Semester:     1,
			Value:        0,
			IsAbsent:     true,
			IsDeleted:    false,
			UpdatedAt:    time.Date(2028, time.Month(11), 12, 14, 30, 40, 0, time.Local),
			SyncedAt:     time.Date(2028, time.Month(11), 12, 14, 35, 13, 0, time.Local),
		}

		redis, redisMock := redismock.NewClientMock()

		studentDisciplineScoresKey := "2028:1:scores:123:234"
		lessonKey := "150:1"

		redisMock.MatchExpectationsInOrder(true)

		redisMock.ExpectWatch(studentDisciplineScoresKey)
		redisMock.ExpectHGet(studentDisciplineScoresKey, lessonKey).SetVal(strconv.Itoa(int(IsAbsentScoreValue)))

		scoresChangesFeedWriter := NewMockScoresChangesFeedWriterInterface(t)

		scoreWriter := ScoreWriter{
			scoresChangesFeedWriter: scoresChangesFeedWriter,
		}
		scoreWriter.setRedis(redis)

		err := scoreWriter.write(&event)

		assert.NoError(t, err)
		assert.NoError(t, redisMock.ExpectationsWereMet())

		scoresChangesFeedWriter.AssertNotCalled(t, "addToQueue")
	})

	t.Run("delete score", func(t *testing.T) {
		event := events.ScoreEvent{
			Id:           112233,
			StudentId:    123,
			LessonId:     150,
			LessonPart:   1,
			DisciplineId: 234,
			Year:         2028,
			Semester:     1,
			Value:        0,
			IsAbsent:     false,
			IsDeleted:    true,
			UpdatedAt:    time.Date(2028, time.Month(11), 12, 14, 30, 40, 0, time.Local),
			SyncedAt:     time.Date(2028, time.Month(11), 12, 14, 35, 13, 0, time.Local),
		}

		redis, redisMock := redismock.NewClientMock()

		studentDisciplineScoresKey := "2028:1:scores:123:234"
		lessonKey := "150:1"

		disciplineTotalsKey := "2028:1:totals:234"
		studentDisciplinesKey := "2028:1:student_disciplines:123"

		redisMock.MatchExpectationsInOrder(true)

		redisMock.ExpectWatch(studentDisciplineScoresKey)

		redisMock.ExpectHGet(studentDisciplineScoresKey, lessonKey).SetVal("3.5")
		redisMock.ExpectTxPipeline()
		redisMock.ExpectHDel(studentDisciplineScoresKey, lessonKey).SetVal(1)

		redisMock.ExpectZIncrBy(disciplineTotalsKey, -3.5, "123").SetVal(1)

		redisMock.ExpectTxPipelineExec()

		redisMock.ExpectSIsMember(studentDisciplinesKey, uint(234)).SetVal(true)

		scoresChangesFeedWriter := NewMockScoresChangesFeedWriterInterface(t)
		scoresChangesFeedWriter.On("addToQueue", event, scoreValue{
			Value:     3.5,
			IsAbsent:  false,
			IsDeleted: false,
		})

		scoreWriter := ScoreWriter{
			scoresChangesFeedWriter: scoresChangesFeedWriter,
		}
		scoreWriter.setRedis(redis)

		err := scoreWriter.write(&event)

		assert.NoError(t, err)
		assert.NoError(t, redisMock.ExpectationsWereMet())
	})

	t.Run("re-write (change) score", func(t *testing.T) {
		event := events.ScoreEvent{
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
			UpdatedAt:    time.Date(2028, time.Month(11), 12, 14, 30, 40, 0, time.Local),
			SyncedAt:     time.Date(2028, time.Month(11), 12, 14, 35, 13, 0, time.Local),
		}

		redis, redisMock := redismock.NewClientMock()

		studentDisciplineScoresKey := "2028:1:scores:123:234"
		lessonKey := "150:1"

		disciplineTotalsKey := "2028:1:totals:234"
		studentDisciplinesKey := "2028:1:student_disciplines:123"

		redisMock.MatchExpectationsInOrder(true)

		redisMock.ExpectWatch(studentDisciplineScoresKey)

		redisMock.ExpectHGet(studentDisciplineScoresKey, lessonKey).SetVal("7")
		redisMock.ExpectTxPipeline()
		redisMock.ExpectHSet(studentDisciplineScoresKey, lessonKey, 2.5).SetVal(1)

		redisMock.ExpectZIncrBy(disciplineTotalsKey, -4.5, "123").SetVal(1)

		redisMock.ExpectTxPipelineExec()

		redisMock.ExpectSIsMember(studentDisciplinesKey, uint(234)).SetVal(false)
		redisMock.ExpectSAdd(studentDisciplinesKey, uint(234)).SetVal(1)

		scoresChangesFeedWriter := NewMockScoresChangesFeedWriterInterface(t)
		scoresChangesFeedWriter.On("addToQueue", event, scoreValue{
			Value:     7,
			IsAbsent:  false,
			IsDeleted: false,
		})

		scoreWriter := ScoreWriter{
			scoresChangesFeedWriter: scoresChangesFeedWriter,
		}
		scoreWriter.setRedis(redis)

		err := scoreWriter.write(&event)

		assert.NoError(t, err)
		assert.NoError(t, redisMock.ExpectationsWereMet())
	})

	t.Run("error on read score", func(t *testing.T) {
		expectedError := errors.New("expected error")

		event := events.ScoreEvent{
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
			UpdatedAt:    time.Date(2028, time.Month(11), 12, 14, 30, 40, 0, time.Local),
			SyncedAt:     time.Date(2028, time.Month(11), 12, 14, 35, 13, 0, time.Local),
		}

		redis, redisMock := redismock.NewClientMock()

		studentDisciplineScoresKey := "2028:1:scores:123:234"
		lessonKey := "150:1"

		redisMock.MatchExpectationsInOrder(true)

		redisMock.ExpectWatch(studentDisciplineScoresKey)
		redisMock.ExpectHGet(studentDisciplineScoresKey, lessonKey).SetErr(expectedError)

		scoresChangesFeedWriter := NewMockScoresChangesFeedWriterInterface(t)

		scoreWriter := ScoreWriter{
			scoresChangesFeedWriter: scoresChangesFeedWriter,
		}
		scoreWriter.setRedis(redis)

		err := scoreWriter.write(&event)

		assert.Error(t, err)
		assert.Equal(t, expectedError, err)

		assert.NoError(t, redisMock.ExpectationsWereMet())
	})

	t.Run("write score error", func(t *testing.T) {
		expectedError := errors.New("expected error")

		event := events.ScoreEvent{
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
			UpdatedAt:    time.Date(2028, time.Month(11), 12, 14, 30, 40, 0, time.Local),
			SyncedAt:     time.Date(2028, time.Month(11), 12, 14, 35, 13, 0, time.Local),
		}

		redis, redisMock := redismock.NewClientMock()

		studentDisciplineScoresKey := "2028:1:scores:123:234"
		lessonKey := "150:1"

		disciplineTotalsKey := "2028:1:totals:234"

		redisMock.MatchExpectationsInOrder(true)

		redisMock.ExpectWatch(studentDisciplineScoresKey)

		redisMock.ExpectHGet(studentDisciplineScoresKey, lessonKey).RedisNil()
		redisMock.ExpectTxPipeline()
		redisMock.ExpectHSet(studentDisciplineScoresKey, lessonKey, 2.5).SetVal(1)

		redisMock.ExpectZIncrBy(disciplineTotalsKey, 2.5, "123").SetVal(1)

		redisMock.ExpectTxPipelineExec().SetErr(expectedError)

		scoresChangesFeedWriter := NewMockScoresChangesFeedWriterInterface(t)

		scoreWriter := ScoreWriter{
			scoresChangesFeedWriter: scoresChangesFeedWriter,
		}
		scoreWriter.setRedis(redis)

		err := scoreWriter.write(&event)

		assert.Error(t, err)
		assert.NoError(t, redisMock.ExpectationsWereMet())
	})

}
