package api

import "strings"

const (
	mlabPrefix = "mlab-"
)

// NewThirdPartyNamer returns a new instance of ThirdPartyNamer.
func NewThirdPartyNamer(dt, exp, project string) *ThirdPartyNamer {
	return &ThirdPartyNamer{
		Datatype:   dt,
		Experiment: exp,
		Project:    strings.TrimPrefix(project, mlabPrefix),
	}
}

// ThirdPartyNamer provides the naming conventions for third-party
// datatypes.
type ThirdPartyNamer struct {
	Datatype   string // Datatype name.
	Experiment string // Experiment name.
	Project    string // Project name.
}

// Dataset name (e.g., "experiment").
func (tp *ThirdPartyNamer) Dataset() string {
	return tp.Experiment
}

// Table name (e.g., "datatype").
func (tp *ThirdPartyNamer) Table() string {
	return tp.Datatype
}

// ViewDataset name (e.g., "project").
func (tp *ThirdPartyNamer) ViewDataset() string {
	return tp.Project
}

// ViewTable name (e.g., "experiment_datatype").
func (tp *ThirdPartyNamer) ViewTable() string {
	return tp.Experiment + "_" + tp.Datatype
}
