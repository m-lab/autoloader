package v3

import (
	"github.com/m-lab/autoloader/api"
)

// Datatype is a v3 datatype. All organizations share its table, and the
// organization of a row is taken from the "org=<name>" component of the
// object path by a hive-partitioned load.
type Datatype struct {
	*api.Datatype
	// HivePrefix is the common source URI prefix of the datatype's data
	// (e.g., "gs://archive-mlab-foo/autoload/v3"), without a trailing slash.
	HivePrefix string
}

// NewDatatype returns a new Datatype with v3 naming conventions.
func NewDatatype(opts api.DatatypeOpts, hivePrefix string) *Datatype {
	return &Datatype{
		Datatype: &api.Datatype{
			DatatypeOpts: opts,
			Namer:        NewNamer(opts),
		},
		HivePrefix: hivePrefix,
	}
}
