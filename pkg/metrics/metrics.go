package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	CurrentOpsTime = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "monstache_current_op_ts",
		Help: "Current op timestamp (op time for oplog, _id time for direct read)",
	})

	OpsReceived = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "monstache_received_ops_total",
		Help: "The total number of received mongodb ops",
	}, []string{"ns", "op"})

	OpsProcessed = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "monstache_processed_ops_total",
		Help: "The total number of processed ops by sink",
	}, []string{"sink"})

	SinkCommitLatencyHistogram = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "monstache_sink_commit_latency",
		Help:    "How long (ms) each sink commit take",
		Buckets: []float64{100, 300, 500, 1000},
	}, []string{
		"sink",
	})
)
