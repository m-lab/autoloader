package api

const (
	rawPrefix = "raw_"
	rawSuffix = "_raw"
)

// NewMlabNamer returns a new instance of MlabNamer.
func NewMlabNamer(dt, exp string) *MlabNamer {
	return &MlabNamer{
		Datatype:   dt,
		Experiment: exp,
	}
}

// MlabNamer provides the naming conventions for M-Lab datatypes.
type MlabNamer struct {
	Datatype   string // Datatype name.
	Experiment string // Experiment name.
}

// Dataset name (e.g., "raw_ndt").
func (m *MlabNamer) Dataset() string {
	return rawPrefix + m.Experiment
}

// Table name (e.g., "ndt7").
func (m *MlabNamer) Table() string {
	return m.Datatype
}

// ViewDataset name (e.g., "ndt_raw").
func (m *MlabNamer) ViewDataset() string {
	return m.Experiment + rawSuffix
}

// ViewTable name (e.g., "ndt7").
func (m *MlabNamer) ViewTable() string {
	return m.Datatype
}
