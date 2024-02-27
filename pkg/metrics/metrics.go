package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	OpsReceived = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "monstache_received_ops_total",
		Help: "The total number of received mongodb oplogs",
	}, []string{"ns", "op"})

	OpsProcessed = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "monstache_processed_ops_total",
		Help: "The total number of processed oplogs by sink",
	}, []string{"sink"})
)
