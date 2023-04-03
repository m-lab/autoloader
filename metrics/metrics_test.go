package metrics

import "testing"

func TestLinltMetrics(t *testing.T) {
	AutoloadDuration.WithLabelValues("experiment", "datatype", "status")
	BigQueryOperationsTotal.WithLabelValues("experiment", "datatype", "operation", "status")
}
