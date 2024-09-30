package main

import (
	"context"
	"errors"
	"fmt"
	"github.com/go-redis/redis/v9"
	"github.com/kneu-messenger-pigeon/events"
	"io"
)

// YearChangeWriter
/*
 * YearChangeWriter is Pseudo writer.
 * Purpose of this writer is to remove all records from Redis related to previous education year
 */
type YearChangeWriter struct {
	out                  io.Writer
	redis                redis.UniversalClient
	isValidEducationYear func(int) bool
}

func (writer *YearChangeWriter) setRedis(redis redis.UniversalClient) {
	writer.redis = redis
}

func (writer *YearChangeWriter) getExpectedMessageKey() string {
	// self check on init that we have correct installed writer
	if writer.isValidEducationYear != nil && writer.out != nil {
		return events.CurrentYearEventName
	}

	return ""
}

func (writer *YearChangeWriter) getExpectedEventType() any {
	return &events.CurrentYearEvent{}
}

func (writer *YearChangeWriter) write(s any) (err error) {
	currentYear := s.(*events.CurrentYearEvent).Year
	if !writer.isValidEducationYear(currentYear) {
		fmt.Fprintf(writer.out, "Skip invalid education year: %d\n", currentYear)
		return nil
	}

	ctx := context.Background()
	previousYear, err := writer.redis.Get(ctx, "currentYear").Int()
	if errors.Is(err, redis.Nil) {
		err = nil
		previousYear = 0
	}

	if previousYear != 0 && previousYear < currentYear {
		iter := writer.redis.Scan(ctx, 0, fmt.Sprintf("%d:*", previousYear), 0).Iterator()

		for err == nil && iter.Next(ctx) {
			err = writer.redis.Del(ctx, iter.Val()).Err()
		}
		if err == nil && iter.Err() != nil {
			err = iter.Err()
		}
	}

	if previousYear < currentYear && err == nil {
		err = writer.redis.Set(ctx, "currentYear", currentYear, 0).Err()
		if err == nil {
			err = writer.redis.Save(ctx).Err()
		}
	}

	return
}

func isValidEducationYear(educationYear int) bool {
	return educationYear >= 2022 && educationYear < 2050
}
