package main

import (
	"context"
	"io"
	"os/signal"
	"runtime"
	"sync"
	"syscall"
)

const ConnectorPoolSize = 6

type EventLoop struct {
	out                     io.Writer
	connectorsPool          [ConnectorPoolSize]ConnectorInterface
	scoresChangesFeedWriter ScoresChangesFeedWriterInterface
}

func (eventLoop *EventLoop) execute() {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT)
	defer stop()

	wg := &sync.WaitGroup{}

	wg.Add(1)
	go func() {
		eventLoop.scoresChangesFeedWriter.execute(ctx)
		wg.Done()
	}()

	wg.Add(len(eventLoop.connectorsPool))
	for _, connector := range eventLoop.connectorsPool {
		go connector.execute(ctx, wg)
	}

	runtime.Gosched()
	wg.Wait()
}
