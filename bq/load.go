package bq

import (
	"context"
	"errors"

	"cloud.google.com/go/bigquery"
	"github.com/googleapis/google-cloud-go-testing/bigquery/bqiface"
	"github.com/m-lab/autoloader/api"
)

// newLoadConfig returns the configuration of a load job that reads JSON files
// from the input GCS uris and overwrites the destination. If the table name
// includes a partition decoration (e.g., table$YYYYMMDD), only said partition
// is overwritten.
//
// With a non-empty hivePrefix, the job uses hive partitioning in STRINGS mode
// with that source URI prefix, and the source carries the input schema,
// because BigQuery rejects a hive-partitioned load that has neither a schema
// nor autodetect. Without a prefix, no schema is set.
func newLoadConfig(hivePrefix string, schema bigquery.Schema, uri ...string) bigquery.LoadConfig {
	gcsRef := bigquery.NewGCSReference(uri...)
	gcsRef.SourceFormat = bigquery.JSON
	cfg := bigquery.LoadConfig{
		Src:              gcsRef,
		WriteDisposition: bigquery.WriteTruncate,
	}
	if hivePrefix != "" {
		gcsRef.Schema = schema
		// The shared table always exists before a load. A load must never
		// create it, because it would lack the partitioning and clustering.
		cfg.CreateDisposition = bigquery.CreateNever
		cfg.HivePartitioningOptions = &bigquery.HivePartitioningOptions{
			Mode:            bigquery.StringHivePartitioningMode,
			SourceURIPrefix: hivePrefix,
		}
	}
	return cfg
}

// Load loads data from a set of GCS uris to a BigQuery table. It overwrites the existing data in
// the destination table. If the table name includes a partition decoration (e.g., table$YYYYMMDD),
// it will only overwrite said partition.
func (c *Client) Load(ctx context.Context, ds bqiface.Dataset, name string, uri ...string) error {
	return load(ctx, ds.Table(name), newLoadConfig("", nil, uri...))
}

// LoadHive loads data from a set of GCS uris under the datatype's hive prefix
// to a BigQuery table, filling the "org" column from the "org=<name>"
// component of each object path. It overwrites the existing data in the
// destination table, or only the partition named by a decoration (e.g.,
// table$YYYYMMDD). The load carries the schema from the schema file, without
// the "org" column.
func (c *Client) LoadHive(ctx context.Context, ds bqiface.Dataset, name string, dt *api.Datatype, uri ...string) error {
	schema, err := bigquery.SchemaFromJSON(dt.Schema)
	if err != nil {
		return err
	}
	return load(ctx, ds.Table(name), newLoadConfig(dt.HivePrefix, schema, uri...))
}

// load runs a load job with the input configuration into tbl and waits for it.
func load(ctx context.Context, tbl bqiface.Table, cfg bigquery.LoadConfig) error {
	loader := tbl.LoaderFrom(cfg.Src)
	loader.SetLoadConfig(bqiface.LoadConfig{
		LoadConfig: cfg,
		Dst:        tbl,
	})

	job, err := loader.Run(ctx)
	if err != nil {
		return err
	}

	status, err := job.Wait(ctx)
	if err != nil {
		return err
	}

	if status.Err() != nil {
		return jobErrors(status)
	}

	return nil
}

func jobErrors(status *bigquery.JobStatus) error {
	var err error
	for _, e := range status.Errors {
		err = errors.Join(err, e)
	}
	return err
}
