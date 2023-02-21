package api

import (
	"time"

	"github.com/m-lab/go/storagex"
)

// Datatype defines an individual datatype whose existence is denoted
// by a schema file under the path `autoload/v1/tables/<Experiment>/<Datatype>.table.json`
// within a GCS bucket.
//
// For each Datatype, there will be a BigQuery table named `<Project>:<Experiment>.<Name>`
// with the contents under `autoload/v1/<Experiment>/<Datatype>/*`.
type Datatype struct {
	Name        string           // Datatype name (e.g., "ndt7")
	Experiment  string           // Experiment name (e.g., "ndt")
	Location    string           // Bucket location (e.g., "us-east")
	Schema      []byte           // Contents of schema file in GCS.
	UpdatedTime time.Time        // Last time the schema was updated in GCS.
	Bucket      *storagex.Bucket // GCS Bucket.
}
