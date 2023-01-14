package main

import (
	"bytes"
	"github.com/go-redis/redismock/v9"
	"github.com/kneu-messenger-pigeon/events"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestYearChangeWriter(t *testing.T) {
	isValidEducationYearMockTrue := func(_ int) bool { return true }
	//	isValidEducationYearMockFalse := func(_ int) bool { return false }

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

}
