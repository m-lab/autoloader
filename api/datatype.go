package api

import (
	"time"

	"github.com/m-lab/go/storagex"
)

// DatatypeOpts contains the base set of fields for an autoloaded datatype.
type DatatypeOpts struct {
	Name         string           // Datatype name (e.g., "ndt7").
	Experiment   string           // Experiment name (e.g., "ndt").
	Organization string           // Organization name (e.g., "mlab").
	Version      string           // Version (e.g., "v2").
	Location     string           // Bucket location (e.g., "us-east").
	Schema       []byte           // Contents of schema file in GCS.
	UpdatedTime  time.Time        // Last time the schema was updated in GCS.
	Bucket       *storagex.Bucket // GCS Bucket.
}

// Namer provides the appropriate naming conventions for a Datatype.
type Namer interface {
	Dataset() string
	Table() string
	ViewDataset() string
	ViewTable() string
}

// Datatype defines an individual datatype whose existence is denoted
// by a schema file under the path `autoload/v1/tables/<Experiment>/<Datatype>.table.json`
// within a GCS bucket.
type Datatype struct {
	DatatypeOpts
	Namer
	// UpdateView indicates whether the view should be updated in case the schema
	// changes.
	UpdateView bool
}

// NewMlabDatatype returns a new Datatype with an MlabNamer.
func NewMlabDatatype(opts DatatypeOpts) *Datatype {
	return &Datatype{
		DatatypeOpts: opts,
		Namer:        NewMlabNamer(opts.Name, opts.Experiment),
	}
}

// NewThirdPartyDatatype returns a new Datatype with a ThirdPartyNamer.
func NewThirdPartyDatatype(opts DatatypeOpts, project string) *Datatype {
	return &Datatype{
		DatatypeOpts: opts,
		Namer:        NewThirdPartyNamer(opts.Name, opts.Experiment, project),
	}
}
