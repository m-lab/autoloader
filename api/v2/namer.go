package v2

import (
	"fmt"
)

// NewNamer provides a new instance of Namer.
func NewNamer(opts DatatypeOpts, sp string) Namer {
	return Namer{
		Datatype:     opts.Datatype,
		Experiment:   opts.Experiment,
		Organization: opts.Organization,
		Version:      opts.Version,
		SubProject:   sp,
	}
}

// Namer provides the naming conventions for a datatype.
type Namer struct {
	Datatype     string // Datatype name (e.g., "ndt7").
	Experiment   string // Experiment name (e.g., "ndt").
	Organization string // Organization name (e.g., "mlab").
	Version      string // Version (e.g., "v2").
	SubProject   string // SubProject (e.g., "mlab", "autojoin").
}

// Dataset name (e.g., "autoload_v2_mlab_ndt").
func (n *Namer) Dataset() string {
	return fmt.Sprintf("autoload_%s_%s_%s", n.Version, n.Organization, n.Experiment)
}

// Table name (e.g., "ndt7_raw").
func (n *Namer) Table() string {
	return n.Datatype + "_raw"
}

// ViewDataset name (e.g., "mlab_v2_ndt").
func (n *Namer) ViewDataset() string {
	return fmt.Sprintf("%s_%s_%s", n.SubProject, n.Version, n.Experiment)
}

// ViewTable name (e.g., "ndt7_raw").
func (n *Namer) ViewTable() string {
	return n.Table()
}
