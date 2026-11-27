package v3

import (
	"testing"
)

func TestNewDatatype(t *testing.T) {
	prefix := "gs://archive-mlab-foo/autoload/v3"
	dt := NewDatatype(ndt7Opts, prefix)

	if dt.HivePrefix != prefix {
		t.Errorf("HivePrefix = %q, want %q", dt.HivePrefix, prefix)
	}
	if dt.Organization != "" {
		t.Errorf("Organization = %q, want empty", dt.Organization)
	}
	if dt.Name != "ndt7" || dt.Experiment != "ndt" || dt.Version != "v3" {
		t.Errorf("unexpected opts: %+v", dt.DatatypeOpts)
	}
	if got := dt.Dataset(); got != "autoload_v3_ndt" {
		t.Errorf("Dataset() = %q, want autoload_v3_ndt", got)
	}
	if got := dt.Table(); got != "ndt7_raw" {
		t.Errorf("Table() = %q, want ndt7_raw", got)
	}
}
