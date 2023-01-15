package main

import (
	"bytes"
	"errors"
	"github.com/go-redis/redismock/v9"
	"github.com/kneu-messenger-pigeon/events"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func TestYearChangeWriter(t *testing.T) {
	isValidEducationYearMockTrue := func(_ int) bool { return true }
	isValidEducationYearMockFalse := func(_ int) bool { return false }

	t.Run("year not changed", func(t *testing.T) {
		out := &bytes.Buffer{}

		event := events.CurrentYearEvent{
			Year: 2031,
		}

		redis, redisMock := redismock.NewClientMock()
		redisMock.MatchExpectationsInOrder(true)

		redisMock.ExpectGet("currentYear").SetVal("2031")

		yearChangeWriter := YearChangeWriter{
			out:                  out,
			isValidEducationYear: isValidEducationYearMockTrue,
		}

		yearChangeWriter.setRedis(redis)
		err := yearChangeWriter.write(&event)

		assert.IsType(t, yearChangeWriter.getExpectedEventType(), &event)
		assert.Equal(t, yearChangeWriter.getExpectedMessageKey(), events.CurrentYearEventName)

		assert.NoError(t, err)
		assert.NoError(t, redisMock.ExpectationsWereMet())
	})

	t.Run("accept new year with empty previous", func(t *testing.T) {
		out := &bytes.Buffer{}

		event := events.CurrentYearEvent{
			Year: 2031,
		}

		redis, redisMock := redismock.NewClientMock()
		redisMock.MatchExpectationsInOrder(true)

		redisMock.ExpectGet("currentYear").RedisNil()
		redisMock.ExpectSet("currentYear", 2031, 0).SetVal("OK")
		redisMock.ExpectSave().SetVal("OK")

		yearChangeWriter := YearChangeWriter{
			out:                  out,
			isValidEducationYear: isValidEducationYearMockTrue,
		}

		yearChangeWriter.setRedis(redis)
		err := yearChangeWriter.write(&event)

		assert.IsType(t, yearChangeWriter.getExpectedEventType(), &event)
		assert.Equal(t, yearChangeWriter.getExpectedMessageKey(), events.CurrentYearEventName)

		assert.NoError(t, err)
		assert.NoError(t, redisMock.ExpectationsWereMet())
	})

	t.Run("accept new year and delete previous years keys", func(t *testing.T) {
		out := &bytes.Buffer{}

		event := events.CurrentYearEvent{
			Year: 2031,
		}

		redis, redisMock := redismock.NewClientMock()
		redisMock.MatchExpectationsInOrder(true)

		redisMock.ExpectGet("currentYear").SetVal("2030")

		redisMock.ExpectScan(0, "2030:*", 0).SetVal([]string{
			"2030:1:scores:213",
			"2030:discipline",
		}, 0)
		redisMock.ExpectDel("2030:1:scores:213").SetVal(1)
		redisMock.ExpectDel("2030:discipline").SetVal(1)

		redisMock.ExpectSet("currentYear", 2031, 0).SetVal("OK")
		redisMock.ExpectSave().SetVal("OK")

		yearChangeWriter := YearChangeWriter{
			out:                  out,
			isValidEducationYear: isValidEducationYearMockTrue,
		}

		yearChangeWriter.setRedis(redis)
		err := yearChangeWriter.write(&event)

		assert.IsType(t, yearChangeWriter.getExpectedEventType(), &event)
		assert.Equal(t, yearChangeWriter.getExpectedMessageKey(), events.CurrentYearEventName)

		assert.NoError(t, err)
		assert.NoError(t, redisMock.ExpectationsWereMet())
	})

	t.Run("error on delete key", func(t *testing.T) {
		expectedError := errors.New("expected error")
		out := &bytes.Buffer{}

		event := events.CurrentYearEvent{
			Year: 2031,
		}

		redis, redisMock := redismock.NewClientMock()
		redisMock.MatchExpectationsInOrder(true)
		redisMock.ExpectGet("currentYear").SetVal("2030")

		redisMock.ExpectScan(0, "2030:*", 0).SetVal([]string{
			"2030:something:213",
			"2030:students_total",
		}, 0)
		redisMock.ExpectDel("2030:something:213").SetErr(expectedError)

		yearChangeWriter := YearChangeWriter{
			out:                  out,
			isValidEducationYear: isValidEducationYearMockTrue,
		}

		yearChangeWriter.setRedis(redis)
		actualErr := yearChangeWriter.write(&event)

		assert.Error(t, actualErr)
		assert.Equal(t, expectedError, actualErr)

		assert.NoError(t, redisMock.ExpectationsWereMet())
	})

	t.Run("error on scan", func(t *testing.T) {
		expectedError := errors.New("expected error")
		out := &bytes.Buffer{}

		event := events.CurrentYearEvent{
			Year: 2031,
		}

		redis, redisMock := redismock.NewClientMock()
		redisMock.MatchExpectationsInOrder(true)
		redisMock.ExpectGet("currentYear").SetVal("2030")

		redisMock.ExpectScan(0, "2030:*", 0).SetErr(expectedError)

		yearChangeWriter := YearChangeWriter{
			out:                  out,
			isValidEducationYear: isValidEducationYearMockTrue,
		}

		yearChangeWriter.setRedis(redis)
		actualErr := yearChangeWriter.write(&event)

		assert.Error(t, actualErr)
		assert.Equal(t, expectedError, actualErr)

		assert.NoError(t, redisMock.ExpectationsWereMet())
	})

	t.Run("invalid new year", func(t *testing.T) {
		out := &bytes.Buffer{}
		event := events.CurrentYearEvent{
			Year: 2006,
		}

		redis, redisMock := redismock.NewClientMock()
		redisMock.MatchExpectationsInOrder(true)

		yearChangeWriter := YearChangeWriter{
			out:                  out,
			isValidEducationYear: isValidEducationYearMockFalse,
		}
		yearChangeWriter.setRedis(redis)
		err := yearChangeWriter.write(&event)

		assert.NoError(t, err)
		assert.NoError(t, redisMock.ExpectationsWereMet())

		assert.Contains(t, out.String(), "Skip invalid education year: 2006")
	})

	t.Run("Not set isValidEducationYear", func(t *testing.T) {
		out := &bytes.Buffer{}

		yearChangeWriter := YearChangeWriter{
			out: out,
		}

		assert.Empty(t, yearChangeWriter.getExpectedMessageKey())
	})
}

func TestIsValidEducationYear(t *testing.T) {
	t.Run("Valid - first semester, current year", func(t *testing.T) {
		assert.True(t, isValidEducationYear(time.Now().Year()))
	})

	t.Run("Valid - second semester, previous calendar year", func(t *testing.T) {
		assert.True(t, isValidEducationYear(time.Now().Year()-1))
	})

	t.Run("Invalid - year in past", func(t *testing.T) {
		assert.False(t, isValidEducationYear(2020))
	})
	t.Run("Invalid - year in future", func(t *testing.T) {
		assert.False(t, isValidEducationYear(2080))
	})
}
