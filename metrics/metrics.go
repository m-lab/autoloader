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
		[]string{"version", "experiment", "datatype", "period", "status"},
	)

	// BigQueryOperationsTotal counts the number of create, update, and load operations
	// that the autoloader performs in BigQuery.
	BigQueryOperationsTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "autoloader_bigquery_operations_total",
			Help: "The number of create, update, and load operations that the autoloader performs.",
		},
		[]string{"version", "experiment", "datatype", "operation", "status"},
	)

	// LoadedDates keeps track of the most recently loaded date for each datatype
	// and job period type (e.g., daily).
	LoadedDates = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "autoloader_loaded_dates",
			Help: "Most recently loaded date for each datatype and job period type.",
		},
		[]string{"version", "experiment", "datatype", "period", "status"},
	)

	// ExcludedOrgsTotal counts the organizations that were left out of a
	// shared table's date partition because their data could not be loaded.
	ExcludedOrgsTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "autoloader_excluded_orgs_total",
			Help: "The number of times an organization was excluded from a shared table's date partition.",
		},
		[]string{"version", "experiment", "datatype", "org"},
	)
)
