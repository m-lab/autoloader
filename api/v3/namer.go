package v3

import (
	"fmt"

	"github.com/m-lab/autoloader/api"
)

// NewNamer provides a new instance of Namer.
func NewNamer(opts api.DatatypeOpts) *Namer {
	return &Namer{
		Datatype:   opts.Name,
		Experiment: opts.Experiment,
		Version:    opts.Version,
	}
}

// Namer provides the naming conventions for a v3 datatype. All organizations
// share the same dataset and table, so the names do not include an organization.
type Namer struct {
	Datatype   string // Datatype name (e.g., "ndt7").
	Experiment string // Experiment name (e.g., "ndt").
	Version    string // Version (e.g., "v3").
}

// Dataset name (e.g., "autoload_v3_ndt").
func (n *Namer) Dataset() string {
	return fmt.Sprintf("autoload_%s_%s", n.Version, n.Experiment)
}

// Table name (e.g., "ndt7_raw").
func (n *Namer) Table() string {
	return n.Datatype + "_raw"
}

// ViewDataset returns the same name as Dataset. v3 has no views; the method
// exists only to satisfy api.Namer.
func (n *Namer) ViewDataset() string {
	return n.Dataset()
}

// ViewTable returns the same name as Table. v3 has no views; the method
// exists only to satisfy api.Namer.
func (n *Namer) ViewTable() string {
	return n.Table()
}
