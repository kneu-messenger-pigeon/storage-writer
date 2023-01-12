package main

import (
	"bytes"
	"context"
	"github.com/stretchr/testify/mock"
	"sync"
	"syscall"
	"testing"
	"time"
)

func TestEventLoopExecute(t *testing.T) {
	t.Run("EventLoop execute", func(t *testing.T) {
		out := &bytes.Buffer{}
		matchContext := mock.MatchedBy(func(ctx context.Context) bool { return true })
		matchWaitGroup := mock.MatchedBy(func(wg *sync.WaitGroup) bool { wg.Done(); return true })

		connector := NewMockConnectorInterface(t)

		scoresChangesFeedWriter := NewMockScoresChangesFeedWriterInterface(t)
		scoresChangesFeedWriter.On("execute", matchContext).Return().Once()

		connector.On("execute", matchContext, matchWaitGroup).Return().Times(ConnectorPoolSize)

		eventloop := EventLoop{
			out: out,
			connectorsPool: [ConnectorPoolSize]ConnectorInterface{
				connector,
				connector,
				connector,
				connector,
			},
			scoresChangesFeedWriter: scoresChangesFeedWriter,
		}

		go func() {
			time.Sleep(time.Nanosecond * 200)
			_ = syscall.Kill(syscall.Getpid(), syscall.SIGINT)
		}()
		eventloop.execute()

		connector.AssertExpectations(t)
		scoresChangesFeedWriter.AssertExpectations(t)
	})
}
