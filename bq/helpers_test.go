package bq

import (
	"context"

	"cloud.google.com/go/bigquery"
	"github.com/googleapis/google-cloud-go-testing/bigquery/bqiface"
	"github.com/m-lab/autoloader/api"
)

const testHivePrefix = "gs://archive-mlab-foo/autoload/v3"

// The handler package depends on these methods through handler.BQClient.
var _ interface {
	LoadHive(context.Context, bqiface.Dataset, string, *api.Datatype, ...string) error
	EnsureStagingDataset(context.Context, *api.Datatype) (bqiface.Dataset, error)
	EnsureStagingTable(context.Context, bqiface.Dataset, *api.Datatype, string) error
	CopyPartitions(context.Context, bqiface.Table, ...bqiface.Table) error
} = &Client{}

// testNamer names a datatype like the v3 namer does, without depending on it.
type testNamer struct {
	dataset, table string
}

func (n testNamer) Dataset() string     { return n.dataset }
func (n testNamer) Table() string       { return n.table }
func (n testNamer) ViewDataset() string { return n.dataset }
func (n testNamer) ViewTable() string   { return n.table }

// hiveDatatype returns a shared-table datatype for ndt7 with the input schema.
func hiveDatatype(schema []byte) *api.Datatype {
	return &api.Datatype{
		DatatypeOpts: api.DatatypeOpts{
			Name:       "ndt7",
			Experiment: "ndt",
			Version:    "v3",
			Schema:     schema,
		},
		Namer:      testNamer{dataset: "autoload_v3_ndt", table: "ndt7_raw"},
		HivePrefix: testHivePrefix,
	}
}

// recordingLoader is a bqiface.Loader that keeps the configuration it is
// given, which bqfake.Loader discards.
type recordingLoader struct {
	bqiface.Loader
	cfg *bqiface.LoadConfig
	job bqiface.Job
	err error
}

func (l *recordingLoader) SetLoadConfig(cfg bqiface.LoadConfig) { l.cfg = &cfg }

func (l *recordingLoader) Run(context.Context) (bqiface.Job, error) { return l.job, l.err }

// copyTable adds CopierFrom, which bqfake.Table lacks, to a bqfake.Table.
type copyTable struct {
	bqiface.Table
	copier *recordingCopier
}

func (t copyTable) CopierFrom(srcs ...bqiface.Table) bqiface.Copier {
	t.copier.from = srcs
	return t.copier
}

// recordingCopier is a bqiface.Copier that keeps the sources it is created
// with and the configuration it is given.
type recordingCopier struct {
	bqiface.Copier
	from []bqiface.Table
	cfg  *bqiface.CopyConfig
	runs int
	job  bqiface.Job
	err  error
}

func (c *recordingCopier) SetCopyConfig(cfg bqiface.CopyConfig) { c.cfg = &cfg }

func (c *recordingCopier) Run(context.Context) (bqiface.Job, error) {
	c.runs++
	return c.job, c.err
}

// recordingClient is a bqiface.Client whose only dataset is ds.
type recordingClient struct {
	bqiface.Client
	ds *recordingDataset
}

func (c recordingClient) Dataset(string) bqiface.Dataset { return c.ds }

// recordingDataset keeps the metadata it is created with, which
// bqfake.Dataset discards.
type recordingDataset struct {
	bqiface.Dataset
	created *bqiface.DatasetMetadata
}

func (d *recordingDataset) Create(ctx context.Context, md *bqiface.DatasetMetadata) error {
	d.created = md
	return d.Dataset.Create(ctx, md)
}

func mustSchema(b []byte) bigquery.Schema {
	s, err := bigquery.SchemaFromJSON(b)
	if err != nil {
		panic(err)
	}
	return s
}
