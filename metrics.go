package main

import "github.com/VictoriaMetrics/metrics"

var (
	realtimeChangeScoresCount = metrics.NewCounter(`realtime_change_scores_count`)

	secondaryChangeScoresCount = metrics.NewCounter(`secondary_change_scores_count`)
)
