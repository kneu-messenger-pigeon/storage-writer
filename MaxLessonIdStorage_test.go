package main

import (
	"context"
	"github.com/go-redis/redismock/v9"
	"github.com/stretchr/testify/assert"
	"runtime"
	"testing"
	"time"
)

func TestMaxLessonId_Set(t *testing.T) {
	redis, redisMock := redismock.NewClientMock()
	redisMock.MatchExpectationsInOrder(true)

	redisMock.ExpectGet(MaxLessonIdRedisKey).SetVal("5")
	maxLessonId := newMaxLessonIdStorage(redis)
	assert.NoError(t, redisMock.ExpectationsWereMet())
	assert.Equal(t, uint(5), maxLessonId.Get())

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*1)

	var actualValue uint

	redisMock.ExpectSet(MaxLessonIdRedisKey, uint(10), 0).SetVal("OK")
	maxLessonId.Set(10)
	assert.NoError(t, redisMock.ExpectationsWereMet())
	assert.Equal(t, uint(10), maxLessonId.Get())

	select {
	case <-ctx.Done():
		t.Error("timeout")
	case actualValue = <-maxLessonId.Changed():
	}
	assert.Equal(t, uint(10), actualValue)

	redisMock.ExpectSet(MaxLessonIdRedisKey, uint(50), 0).SetVal("OK")
	maxLessonId.Set(50)
	assert.NoError(t, redisMock.ExpectationsWereMet())
	assert.Equal(t, uint(50), maxLessonId.Get())

	select {
	case <-ctx.Done():
		t.Error("timeout")
	case actualValue = <-maxLessonId.Changed():
	}
	assert.Equal(t, uint(50), actualValue)

	redisMock.ClearExpect()
	maxLessonId.Set(30)
	assert.NoError(t, redisMock.ExpectationsWereMet())
	assert.Equal(t, uint(50), maxLessonId.Get())

	runtime.Gosched()
	cancel()
	time.Sleep(time.Millisecond * 100)
}
