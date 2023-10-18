package v2

import (
	"strings"
	"time"

	"github.com/m-lab/go/storagex"
)

// DatatypeOpts contains the base set of fields for an autoloaded datatype.
type DatatypeOpts struct {
	Datatype     string           // Datatype name (e.g., "ndt7").
	Experiment   string           // Experiment name (e.g., "ndt").
	Organization string           // Organization name (e.g., "mlab").
	Version      string           // Version (e.g., "v2").
	Location     string           // Bucket location (e.g., "us-east").
	Schema       []byte           // Contents of schema file in GCS.
	UpdatedTime  time.Time        // Last time the schema was updated in GCS.
	Bucket       *storagex.Bucket // GCS bucket.
}

// Datatype defines an individual datatype within a GCS bucket.
type Datatype struct {
	DatatypeOpts
	Namer
}

// NewMlabDatatype returns a new Datatype with M-Lab naming conventions.
func NewMlabDatatype(opts DatatypeOpts) *Datatype {
	return &Datatype{
		DatatypeOpts: opts,
		Namer:        NewNamer(opts, "mlab"),
	}
}

// NewBYODatatype returns a new Datatype with BYOS/BYOD naming conventions.
func NewBYODatatype(opts DatatypeOpts, project string) *Datatype {
	sp := strings.TrimPrefix(project, "mlab-")
	return &Datatype{
		DatatypeOpts: opts,
		Namer:        NewNamer(opts, sp),
	}
}
