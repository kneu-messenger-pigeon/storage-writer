package main

import (
	"github.com/go-redis/redismock/v9"
	"github.com/kneu-messenger-pigeon/events"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func TestWriteLesson(t *testing.T) {
	t.Run("write lesson", func(t *testing.T) {
		event := events.LessonEvent{
			Id:           600,
			DisciplineId: 200,
			TypeId:       5,
			Date:         time.Date(2027, time.Month(5), 13, 0, 0, 0, 0, time.Local),
			Year:         2026,
			Semester:     2,
			IsDeleted:    false,
		}

		redis, redisMock := redismock.NewClientMock()
		redisMock.ExpectHSet("2026:2:lessons:200", "600", "2705135").SetVal(1)

		lessonWriter := LessonWriter{}

		lessonWriter.setRedis(redis)
		err := lessonWriter.write(&event)

		assert.IsType(t, lessonWriter.getExpectedEventType(), &event)
		assert.Equal(t, lessonWriter.getExpectedMessageKey(), events.LessonEventName)

		assert.NoError(t, err)
		assert.NoError(t, redisMock.ExpectationsWereMet())
	})

	t.Run("delete lesson", func(t *testing.T) {
		event := events.LessonEvent{
			Id:           650,
			DisciplineId: 250,
			TypeId:       5,
			Date:         time.Date(2030, time.Month(4), 26, 0, 0, 0, 0, time.Local),
			Year:         2029,
			Semester:     2,
			IsDeleted:    true,
		}

		redis, redisMock := redismock.NewClientMock()
		redisMock.MatchExpectationsInOrder(true)

		redisMock.ExpectSetEx("2029:2:deleted-lessons:250:650", "3004265", time.Hour*24).SetVal("OK")
		redisMock.ExpectHDel("2029:2:lessons:250", "650").SetVal(1)

		lessonWriter := LessonWriter{}

		lessonWriter.setRedis(redis)
		err := lessonWriter.write(&event)

		assert.IsType(t, lessonWriter.getExpectedEventType(), &event)
		assert.Equal(t, lessonWriter.getExpectedMessageKey(), events.LessonEventName)

		assert.NoError(t, err)
		assert.NoError(t, redisMock.ExpectationsWereMet())
	})
}
