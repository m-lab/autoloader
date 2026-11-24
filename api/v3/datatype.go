package v3

import (
	"github.com/m-lab/autoloader/api"
)

// NewDatatype returns a new Datatype with v3 naming conventions. The hivePrefix
// is the common source URI prefix of the datatype's data (e.g.,
// "gs://archive-mlab-foo/autoload/v3"), without a trailing slash.
func NewDatatype(opts api.DatatypeOpts, hivePrefix string) *api.Datatype {
	return &api.Datatype{
		DatatypeOpts: opts,
		Namer:        NewNamer(opts),
		UpdateView:   false,
		HivePrefix:   hivePrefix,
	}
}
