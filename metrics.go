package main

import "github.com/VictoriaMetrics/metrics"

var (
	realtimeScoresChangesCount = metrics.NewCounter(`scores__changes_count{source="realtime"}`)

	secondaryScoresChangesCount = metrics.NewCounter(`scores__changes_count{source="secondary"}`)
)
