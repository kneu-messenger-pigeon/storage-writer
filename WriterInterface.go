package main

import "github.com/go-redis/redis/v9"

type WriterInterface interface {
	setRedis(redis redis.UniversalClient)
	getExpectedMessageKey() string
	getExpectedEventType() any
	write(event any) error
}
