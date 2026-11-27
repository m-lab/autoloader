package v3

import (
	"testing"

	"github.com/m-lab/autoloader/api"
)

var ndt7Opts = api.DatatypeOpts{
	Name:       "ndt7",
	Experiment: "ndt",
	Version:    "v3",
}

func TestNamer(t *testing.T) {
	var n api.Namer = NewNamer(ndt7Opts)

	tests := []struct {
		name string
		got  string
		want string
	}{
		{"Dataset", n.Dataset(), "autoload_v3_ndt"},
		{"Table", n.Table(), "ndt7_raw"},
		{"ViewDataset", n.ViewDataset(), "autoload_v3_ndt"},
		{"ViewTable", n.ViewTable(), "ndt7_raw"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.got != tt.want {
				t.Errorf("%s() = %q, want %q", tt.name, tt.got, tt.want)
			}
		})
	}
}
