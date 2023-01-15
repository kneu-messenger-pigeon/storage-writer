package main

import (
	"context"
	"fmt"
	"github.com/go-redis/redis/v9"
	"github.com/kneu-messenger-pigeon/events"
	"io"
	"time"
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
	year := s.(*events.CurrentYearEvent).Year
	if !writer.isValidEducationYear(year) {
		fmt.Fprintf(writer.out, "Skip invalid education year: %d\n", year)
		return nil
	}

	ctx := context.Background()
	for previousYear := year - 3; previousYear < year; previousYear++ {
		iter := writer.redis.Scan(ctx, 0, fmt.Sprintf("%d:*", previousYear), 0).Iterator()
		for err == nil && iter.Next(ctx) {
			err = writer.redis.Del(ctx, iter.Val()).Err()
		}
		if err != nil {
			return err
		}
		if iter.Err() != nil {
			return iter.Err()
		}
	}

	return writer.redis.Save(ctx).Err()
}

func isValidEducationYear(educationYear int) bool {
	yearNow := time.Now().Year()
	return educationYear >= 2022 && (educationYear == yearNow-1 || educationYear == yearNow)
}
