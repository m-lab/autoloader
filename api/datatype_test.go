package api

import (
	"testing"
	"time"

	"cloud.google.com/go/storage"
	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/m-lab/go/storagex"
)

var (
	opts = DatatypeOpts{
		Name:        "datatype",
		Experiment:  "experiment",
		Location:    "US",
		Schema:      []byte{},
		UpdatedTime: time.Time{},
		Bucket:      &storagex.Bucket{},
	}
)

func TestNewMlabDatatype(t *testing.T) {
	want := &Datatype{
		DatatypeOpts: opts,
		Namer:        NewMlabNamer(opts.Name, opts.Experiment),
	}

	got := NewMlabDatatype(opts)
	if !cmp.Equal(got, want, cmpopts.IgnoreUnexported(storagex.Bucket{}, storage.BucketHandle{})) {
		t.Errorf("NewMlabDatatype() = %v, want %v", got, want)
	}
}

func TestNewThirdPartyDatatype(t *testing.T) {
	want := &Datatype{
		DatatypeOpts: opts,
		Namer:        NewThirdPartyNamer(opts.Name, opts.Experiment, "project"),
	}

	got := NewThirdPartyDatatype(opts, "project")
	if !cmp.Equal(got, want, cmpopts.IgnoreUnexported(storagex.Bucket{}, storage.BucketHandle{})) {
		t.Errorf("NewThirdPartyDatatype() = %v, want %v", got, want)
	}
}
