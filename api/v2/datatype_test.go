package v2

import (
	"testing"
	"time"

	"cloud.google.com/go/storage"
	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/m-lab/autoloader/api"
	"github.com/m-lab/go/storagex"
)

var opts = api.DatatypeOpts{
	Name:         "datatype",
	Experiment:   "experiment",
	Organization: "organization",
	Version:      "version",
	Location:     "location",
	Schema:       []byte{},
	UpdatedTime:  time.Time{},
	Bucket:       &storagex.Bucket{},
}

func TestNewMlabDatatype(t *testing.T) {
	want := &api.Datatype{
		DatatypeOpts: opts,
		Namer:        NewNamer(opts, "mlab"),
	}

	got := NewMlabDatatype(opts)
	if !cmp.Equal(got, want, cmpopts.IgnoreUnexported(storagex.Bucket{}, storage.BucketHandle{})) {
		t.Errorf("NewMlabDatatype() = %v, want %v", got, want)
	}
}

func TestNewBYODatatype(t *testing.T) {
	want := &api.Datatype{
		DatatypeOpts: opts,
		Namer:        NewNamer(opts, "subproject"),
	}

	got := NewBYODatatype(opts, "mlab-subproject")
	if !cmp.Equal(got, want, cmpopts.IgnoreUnexported(storagex.Bucket{}, storage.BucketHandle{})) {
		t.Errorf("NewBYODatatype() = %v, want %v", got, want)
	}
}
