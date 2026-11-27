package metrics

import "testing"

func TestLintMetrics(t *testing.T) {
	AutoloadDuration.WithLabelValues("version", "experiment", "datatype", "period", "status")
	BigQueryOperationsTotal.WithLabelValues("version", "experiment", "datatype", "operation", "status")
	LoadedDates.WithLabelValues("version", "experiment", "datatype", "period", "status")
	LoadErrorsTotal.WithLabelValues("version", "experiment", "datatype", "org", "reason")
}
