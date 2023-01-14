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

	t.Run("accept new year and delete previous years keys", func(t *testing.T) {
		out := &bytes.Buffer{}

		event := events.CurrentYearEvent{
			Year: 2031,
		}

		redis, redisMock := redismock.NewClientMock()
		redisMock.MatchExpectationsInOrder(true)

		redisMock.ExpectScan(0, "2028:*", 0).SetVal([]string{
			"2028:2:scores:999",
			"2028:2:discipline",
		}, 0)
		redisMock.ExpectDel("2028:2:scores:999").SetVal(1)
		redisMock.ExpectDel("2028:2:discipline").SetVal(1)

		redisMock.ExpectScan(0, "2029:*", 0).SetVal([]string{
			"2029:something:213",
			"2029:students_total",
		}, 0)
		redisMock.ExpectDel("2029:something:213").SetVal(1)
		redisMock.ExpectDel("2029:students_total").SetVal(1)

		redisMock.ExpectScan(0, "2030:*", 0).SetVal([]string{
			"2030:1:scores:213",
			"2030:discipline",
		}, 0)
		redisMock.ExpectDel("2030:1:scores:213").SetVal(1)
		redisMock.ExpectDel("2030:discipline").SetVal(1)

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

		redisMock.ExpectScan(0, "2028:*", 0).SetVal([]string{
			"2028:2:scores:999",
			"2028:2:discipline",
		}, 0)
		redisMock.ExpectDel("2028:2:scores:999").SetVal(1)
		redisMock.ExpectDel("2028:2:discipline").SetVal(1)

		redisMock.ExpectScan(0, "2029:*", 0).SetVal([]string{
			"2029:something:213",
			"2029:students_total",
		}, 0)
		redisMock.ExpectDel("2029:something:213").SetErr(expectedError)

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

		redisMock.ExpectScan(0, "2028:*", 0).SetVal([]string{
			"2028:2:scores:999",
			"2028:2:discipline",
		}, 0)
		redisMock.ExpectDel("2028:2:scores:999").SetVal(1)
		redisMock.ExpectDel("2028:2:discipline").SetVal(1)

		redisMock.ExpectScan(0, "2029:*", 0).SetErr(expectedError)

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
