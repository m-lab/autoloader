package v2

import (
	"testing"
	"time"

	"github.com/m-lab/go/storagex"
)

var (
	moosOpts = DatatypeOpts{
		Datatype:     "ndt7",
		Experiment:   "ndt",
		Organization: "mlab",
		Version:      "v2",
		Location:     "US",
		Schema:       []byte{},
		UpdatedTime:  time.Time{},
		Bucket:       &storagex.Bucket{},
	}

	pyodOpts = DatatypeOpts{
		Datatype:     "thirdpartydt",
		Experiment:   "thirdpartyexp",
		Organization: "mlab",
		Version:      "v2",
		Location:     "US",
		Schema:       []byte{},
		UpdatedTime:  time.Time{},
		Bucket:       &storagex.Bucket{},
	}

	byosOpts = DatatypeOpts{
		Datatype:     "ndt7",
		Experiment:   "ndt",
		Organization: "thirdpartyorg",
		Version:      "v2",
		Location:     "US",
		Schema:       []byte{},
		UpdatedTime:  time.Time{},
		Bucket:       &storagex.Bucket{},
	}

	byodOpts = DatatypeOpts{
		Datatype:     "thirdpartydt",
		Experiment:   "thirdpartyexp",
		Organization: "thirdpartyorg",
		Version:      "v2",
		Location:     "US",
		Schema:       []byte{},
		UpdatedTime:  time.Time{},
		Bucket:       &storagex.Bucket{},
	}
)

func TestNamer_Dataset(t *testing.T) {
	tests := []struct {
		name string
		opts DatatypeOpts
		sp   string
		want string
	}{
		{
			name: "moos",
			opts: moosOpts,
			sp:   "mlab",
			want: "autoload_v2_mlab_ndt",
		},
		{
			name: "pyod",
			opts: pyodOpts,
			sp:   "mlab",
			want: "autoload_v2_mlab_thirdpartyexp",
		},
		{
			name: "byos",
			opts: byosOpts,
			sp:   "autojoin",
			want: "autoload_v2_thirdpartyorg_ndt",
		},
		{
			name: "byod",
			opts: byodOpts,
			sp:   "thirdparty",
			want: "autoload_v2_thirdpartyorg_thirdpartyexp",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			n := NewNamer(tt.opts, tt.sp)
			if got := n.Dataset(); got != tt.want {
				t.Errorf("Namer.Dataset() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestNamer_Table(t *testing.T) {
	tests := []struct {
		name string
		opts DatatypeOpts
		sp   string
		want string
	}{
		{
			name: "moos",
			opts: moosOpts,
			sp:   "mlab",
			want: "ndt7_raw",
		},
		{
			name: "pyod",
			opts: pyodOpts,
			sp:   "mlab",
			want: "thirdpartydt_raw",
		},
		{
			name: "byos",
			opts: byosOpts,
			sp:   "autojoin",
			want: "ndt7_raw",
		},
		{
			name: "byod",
			opts: byodOpts,
			sp:   "thirdparty",
			want: "thirdpartydt_raw",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			n := NewNamer(tt.opts, tt.sp)
			if got := n.Table(); got != tt.want {
				t.Errorf("Namer.Table() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestNamer_ViewDataset(t *testing.T) {
	tests := []struct {
		name string
		opts DatatypeOpts
		sp   string
		want string
	}{
		{
			name: "moos",
			opts: moosOpts,
			sp:   "mlab",
			want: "mlab_v2_ndt",
		},
		{
			name: "pyod",
			opts: pyodOpts,
			sp:   "mlab",
			want: "mlab_v2_thirdpartyexp",
		},
		{
			name: "byos",
			opts: byosOpts,
			sp:   "autojoin",
			want: "autojoin_v2_ndt",
		},
		{
			name: "byod",
			opts: byodOpts,
			sp:   "thirdparty",
			want: "thirdparty_v2_thirdpartyexp",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			n := NewNamer(tt.opts, tt.sp)
			if got := n.ViewDataset(); got != tt.want {
				t.Errorf("Namer.ViewDataset() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestNamer_ViewTable(t *testing.T) {
	tests := []struct {
		name string
		opts DatatypeOpts
		sp   string
		want string
	}{
		{
			name: "moos",
			opts: moosOpts,
			sp:   "mlab",
			want: "ndt7_raw",
		},
		{
			name: "pyod",
			opts: pyodOpts,
			sp:   "mlab",
			want: "thirdpartydt_raw",
		},
		{
			name: "byos",
			opts: byosOpts,
			sp:   "autojoin",
			want: "ndt7_raw",
		},
		{
			name: "byod",
			opts: byodOpts,
			sp:   "thirdparty",
			want: "thirdpartydt_raw",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			n := NewNamer(tt.opts, tt.sp)
			if got := n.ViewTable(); got != tt.want {
				t.Errorf("Namer.ViewTable() = %v, want %v", got, tt.want)
			}
		})
	}
}
