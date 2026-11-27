package bq

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"cloud.google.com/go/bigquery"
	"github.com/googleapis/google-cloud-go-testing/bigquery/bqiface"
	apiv3 "github.com/m-lab/autoloader/api/v3"
)

// This file holds the BigQuery operations of v3 datatypes: all organizations
// of a datatype are loaded into one table. v1 and v2 do not use it.

// orgColumn is the column of a v3 table that holds the organization,
// taken from the "org=<name>" component of the object path.
const orgColumn = "org"

// ErrOrgColumn is returned when the schema file of a v3 datatype defines the
// column that the autoloader adds itself.
var ErrOrgColumn = errors.New("schema must not define an org column")

// schemaV3 returns the schema of a v3 table: the schema file's fields
// followed by a nullable STRING column named "org". A schema file that
// already has a top-level "org" column, in any letter case, is an error
// wrapping ErrOrgColumn.
func schemaV3(dt *apiv3.Datatype) (bigquery.Schema, error) {
	schema, err := bigquery.SchemaFromJSON(dt.Schema)
	if err != nil {
		return nil, err
	}
	for _, f := range schema {
		// BigQuery column names are case-insensitive.
		if strings.EqualFold(f.Name, orgColumn) {
			return nil, fmt.Errorf("%w: found %q", ErrOrgColumn, f.Name)
		}
	}
	return append(schema, &bigquery.FieldSchema{
		Name: orgColumn,
		Type: bigquery.StringFieldType,
	}), nil
}

// tableMetadataV3 returns the metadata used to create a v3 table:
// the schema from schemaV3, daily partitioning on the "date" column with
// a required partition filter, and clustering by the "org" column.
func tableMetadataV3(dt *apiv3.Datatype) (*bigquery.TableMetadata, error) {
	schema, err := schemaV3(dt)
	if err != nil {
		return nil, err
	}
	return &bigquery.TableMetadata{
		Name:   dt.Table(),
		Schema: schema,
		TimePartitioning: &bigquery.TimePartitioning{
			Type:                   bigquery.DayPartitioningType,
			Field:                  "date",
			RequirePartitionFilter: true,
		},
		Clustering: &bigquery.Clustering{Fields: []string{orgColumn}},
	}, nil
}

// hiveLoadConfig returns the configuration of a load job that reads JSON
// files from the input GCS uris under hivePrefix and overwrites the
// destination, filling the "org" column from the object paths. If the table
// name includes a partition decoration (e.g., table$YYYYMMDD), only said
// partition is overwritten.
func hiveLoadConfig(hivePrefix string, schema bigquery.Schema, uri ...string) bigquery.LoadConfig {
	gcsRef := bigquery.NewGCSReference(uri...)
	gcsRef.SourceFormat = bigquery.JSON
	// BigQuery rejects a hive-partitioned load that has neither a schema nor
	// autodetect, even when the destination table exists.
	gcsRef.Schema = schema
	return bigquery.LoadConfig{
		Src:              gcsRef,
		WriteDisposition: bigquery.WriteTruncate,
		// The table always exists before a load. A load must never create it,
		// because it would lack the partitioning and clustering.
		CreateDisposition: bigquery.CreateNever,
		HivePartitioningOptions: &bigquery.HivePartitioningOptions{
			Mode:            bigquery.StringHivePartitioningMode,
			SourceURIPrefix: hivePrefix,
		},
	}
}

// CreateTableV3 creates the table of a v3 datatype. It returns the
// table's metadata and an error if the table creation was not successful.
func (c *Client) CreateTableV3(ctx context.Context, ds bqiface.Dataset, dt *apiv3.Datatype) (*bigquery.TableMetadata, error) {
	md, err := tableMetadataV3(dt)
	if err != nil {
		return nil, err
	}

	t := ds.Table(dt.Table())
	err = t.Create(ctx, md)
	if err != nil {
		return nil, err
	}
	return t.Metadata(ctx)
}

// UpdateSchemaV3 updates the schema of the table of a v3 datatype.
func (c *Client) UpdateSchemaV3(ctx context.Context, ds bqiface.Dataset, dt *apiv3.Datatype) error {
	schema, err := schemaV3(dt)
	if err != nil {
		return err
	}

	_, err = ds.Table(dt.Table()).Update(ctx, bigquery.TableMetadataToUpdate{
		Schema: schema,
	}, "")
	return err
}

// LoadHive loads data from a set of GCS uris under the datatype's hive prefix
// to a BigQuery table, filling the "org" column from the "org=<name>"
// component of each object path. It overwrites the existing data in the
// destination table, or only the partition named by a decoration (e.g.,
// table$YYYYMMDD). The load carries the schema from the schema file, without
// the "org" column.
func (c *Client) LoadHive(ctx context.Context, ds bqiface.Dataset, name string, dt *apiv3.Datatype, uri ...string) error {
	schema, err := bigquery.SchemaFromJSON(dt.Schema)
	if err != nil {
		return err
	}

	cfg := hiveLoadConfig(dt.HivePrefix, schema, uri...)
	tbl := ds.Table(name)
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
