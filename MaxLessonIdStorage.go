package main

import (
	"context"
	"github.com/go-redis/redis/v9"
	"sync"
)

const MaxLessonIdRedisKey = "max-lesson-id"

type MaxLessonId struct {
	maxLessonId uint
	sync.Mutex
	changedChannel chan uint
	redis          redis.UniversalClient
}

type MaxLessonIdSetterInterface interface {
	Set(uint)
}

type MaxLessonIdGetterInterface interface {
	Get() uint
	Changed() <-chan uint
}

func newMaxLessonIdStorage(redis redis.UniversalClient) *MaxLessonId {
	maxLessonId, _ := redis.Get(context.Background(), MaxLessonIdRedisKey).Uint64()

	return &MaxLessonId{
		maxLessonId:    uint(maxLessonId),
		changedChannel: make(chan uint, 10),
		redis:          redis,
	}
}

func (storage *MaxLessonId) Get() uint {
	return storage.maxLessonId
}

func (storage *MaxLessonId) Changed() <-chan uint {
	return storage.changedChannel
}

func (storage *MaxLessonId) Set(lessonId uint) {
	if lessonId > storage.maxLessonId {
		storage.Mutex.Lock()
		if lessonId > storage.maxLessonId {
			storage.maxLessonId = lessonId
			storage.redis.Set(context.Background(), MaxLessonIdRedisKey, storage.maxLessonId, 0)
		}
		storage.Mutex.Unlock()

		storage.changedChannel <- lessonId
	}
}
