package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	// AutoloadDuration is a histogram of the latency of the autoloading process
	// for each datatype.
	AutoloadDuration = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name: "autoloader_duration",
			Help: "A histogram of autoload latency for each datatype.",
			Buckets: []float64{
				10, 21.5, 46.4,
				100, 215, 464,
				1000, 2150, 4640,
				10000, 21500, 46400,
			},
		},
		[]string{"experiment", "datatype", "status"},
	)

	// BigQueryOperationsTotal counts the number of create, update, and load operations
	// that the autoloader performs in BigQuery.
	BigQueryOperationsTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "autoloader_bigquery_operations_total",
			Help: "The number of create, update, and load operations that the autoloader performs.",
		},
		[]string{"experiment", "datatype", "operation", "status"},
	)
)
