package metrics

import "testing"

func TestLintMetrics(t *testing.T) {
	AutoloadDuration.WithLabelValues("experiment", "datatype", "period", "status")
	BigQueryOperationsTotal.WithLabelValues("experiment", "datatype", "operation", "status")
	LoadedDates.WithLabelValues("experiment", "datatype", "period", "status")
}
