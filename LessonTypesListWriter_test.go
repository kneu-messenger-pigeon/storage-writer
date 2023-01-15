package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"github.com/go-redis/redismock/v9"
	"github.com/kneu-messenger-pigeon/events"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestLessonTypesListWriter(t *testing.T) {
	t.Run("accept new list, save into redis and call Redis background save", func(t *testing.T) {
		out := &bytes.Buffer{}
		event := events.LessonTypesList{
			Year: 2031,
			List: []struct {
				Id        int
				ShortName string
				LongName  string
			}{
				{
					Id:        20,
					ShortName: "Лек",
					LongName:  "Лекція",
				},
			},
		}

		expectedString, _ := json.Marshal(event.List)

		redis, redisMock := redismock.NewClientMock()
		redisMock.MatchExpectationsInOrder(true)

		redisMock.ExpectGetSet("lessonTypes", expectedString).SetVal("")
		redisMock.ExpectBgSave().SetVal("OK")

		lessonTypesListWriter := LessonTypesListWriter{
			out: out,
		}

		lessonTypesListWriter.setRedis(redis)
		err := lessonTypesListWriter.write(&event)

		assert.IsType(t, lessonTypesListWriter.getExpectedEventType(), &event)
		assert.Equal(t, lessonTypesListWriter.getExpectedMessageKey(), events.LessonTypesListName)

		assert.NoError(t, err)
		assert.NoError(t, redisMock.ExpectationsWereMet())
	})

	t.Run("accept new list which equal to stored in Redis", func(t *testing.T) {
		out := &bytes.Buffer{}

		event := events.LessonTypesList{
			Year: 2031,
			List: []struct {
				Id        int
				ShortName string
				LongName  string
			}{
				{
					Id:        20,
					ShortName: "Лек",
					LongName:  "Лекція",
				},
			},
		}

		expectedString, _ := json.Marshal(event.List)

		redis, redisMock := redismock.NewClientMock()
		redisMock.MatchExpectationsInOrder(true)

		redisMock.ExpectGetSet("lessonTypes", expectedString).SetVal(string(expectedString))

		lessonTypesListWriter := LessonTypesListWriter{
			out: out,
		}

		lessonTypesListWriter.setRedis(redis)
		err := lessonTypesListWriter.write(&event)

		assert.IsType(t, lessonTypesListWriter.getExpectedEventType(), &event)
		assert.Equal(t, lessonTypesListWriter.getExpectedMessageKey(), events.LessonTypesListName)

		assert.NoError(t, err)
		assert.NoError(t, redisMock.ExpectationsWereMet())
	})

	t.Run("error on write key", func(t *testing.T) {
		expectedError := errors.New("expected error")
		out := &bytes.Buffer{}

		event := events.LessonTypesList{
			Year: 2031,
			List: []struct {
				Id        int
				ShortName string
				LongName  string
			}{
				{
					Id:        20,
					ShortName: "Лек",
					LongName:  "Лекція",
				},
			},
		}

		expectedString, _ := json.Marshal(event.List)

		redis, redisMock := redismock.NewClientMock()
		redisMock.MatchExpectationsInOrder(true)
		redisMock.ExpectGetSet("lessonTypes", expectedString).SetErr(expectedError)

		lessonTypesListWriter := LessonTypesListWriter{
			out: out,
		}

		lessonTypesListWriter.setRedis(redis)
		actualErr := lessonTypesListWriter.write(&event)

		assert.Error(t, actualErr)
		assert.Equal(t, expectedError, actualErr)

		assert.NoError(t, redisMock.ExpectationsWereMet())
	})

	t.Run("error on bgscan", func(t *testing.T) {
		expectedError := errors.New("expected error")
		out := &bytes.Buffer{}
		event := events.LessonTypesList{
			Year: 2031,
			List: []struct {
				Id        int
				ShortName string
				LongName  string
			}{
				{
					Id:        20,
					ShortName: "Лек",
					LongName:  "Лекція",
				},
			},
		}

		expectedString, _ := json.Marshal(event.List)

		redis, redisMock := redismock.NewClientMock()
		redisMock.MatchExpectationsInOrder(true)

		redisMock.ExpectGetSet("lessonTypes", expectedString).SetVal("")
		redisMock.ExpectBgSave().SetErr(expectedError)

		lessonTypesListWriter := LessonTypesListWriter{
			out: out,
		}

		lessonTypesListWriter.setRedis(redis)
		actualErr := lessonTypesListWriter.write(&event)

		assert.Error(t, actualErr)
		assert.Equal(t, expectedError, actualErr)

		assert.NoError(t, redisMock.ExpectationsWereMet())
	})

}
