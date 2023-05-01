package bq

import (
	"context"

	"cloud.google.com/go/bigquery"
	"github.com/googleapis/google-cloud-go-testing/bigquery/bqiface"
	"github.com/m-lab/autoloader/api"
)

// Client is used to perform BigQuery operations.
type Client struct {
	bqiface.Client
	ViewClient bqiface.Client
}

// NewClient returns a new instance of Client.
func NewClient(c *bigquery.Client, vc *bigquery.Client) *Client {
	return &Client{
		Client:     bqiface.AdaptClient(c),
		ViewClient: bqiface.AdaptClient(vc),
	}
}

// GetDataset returns a handle to the input dataset and an error indicating whether the
// dataset exists.
func (c *Client) GetDataset(ctx context.Context, name string) (bqiface.Dataset, error) {
	ds := c.Dataset(name)
	_, err := ds.Metadata(ctx)
	return ds, err
}

// CreateDataset creates a new dataset for the input `api.Datatype`.
// It returns an error if the dataset already exists.
func (c *Client) CreateDataset(ctx context.Context, dt *api.Datatype) (bqiface.Dataset, error) {
	ds := c.Dataset(dt.Dataset())
	err := ds.Create(ctx, &bqiface.DatasetMetadata{
		DatasetMetadata: bigquery.DatasetMetadata{
			Name:     dt.Dataset(),
			Location: dt.Location,
		},
	})
	return ds, err
}

// GetTableMetadata returns the metadata for the input table and an error indicating whether
// the table exists.
func (c *Client) GetTableMetadata(ctx context.Context, ds bqiface.Dataset, name string) (*bigquery.TableMetadata, error) {
	t := ds.Table(name)
	md, err := t.Metadata(ctx)
	return md, err
}

// CreateTable creates a new date-partitioned table for the input `api.Datatype`.
// It returns the table's metadata and an error if the table creation was not successful.
func (c *Client) CreateTable(ctx context.Context, ds bqiface.Dataset, dt *api.Datatype) (*bigquery.TableMetadata, error) {
	bqSchema, err := bigquery.SchemaFromJSON(dt.Schema)
	if err != nil {
		return nil, err
	}

	t := ds.Table(dt.Table())
	err = t.Create(ctx, &bigquery.TableMetadata{
		Name:   dt.Table(),
		Schema: bqSchema,
		TimePartitioning: &bigquery.TimePartitioning{
			Type:                   bigquery.DayPartitioningType,
			Field:                  "date",
			RequirePartitionFilter: true,
		},
	})
	if err != nil {
		return nil, err
	}
	return t.Metadata(ctx)
}

// UpdateSchema updates the schema for the input `api.Datatype` table.
func (c *Client) UpdateSchema(ctx context.Context, ds bqiface.Dataset, dt *api.Datatype) error {
	bqSchema, err := bigquery.SchemaFromJSON(dt.Schema)
	if err != nil {
		return err
	}

	t := ds.Table(dt.Table())
	_, err = t.Update(ctx, bigquery.TableMetadataToUpdate{
		Schema: bqSchema,
	}, "")
	if err != nil {
		return err
	}

	return c.updateView(ctx, dt, bqSchema)
}

func (c *Client) updateView(ctx context.Context, dt *api.Datatype, bqSchema bigquery.Schema) error {
	ds := c.ViewClient.Dataset(dt.ViewDataset())
	_, err := ds.Metadata(ctx)
	if err != nil {
		// Dataset doesn't exist. Nothing to update.
		// Error could also mean insufficient permissions.
		return nil
	}

	view := ds.Table(dt.ViewTable())
	_, err = view.Metadata(ctx)
	if err != nil {
		// View doesn't exist. Nothing to update.
		// Error could also mean insufficient permissions.
		return nil
	}

	_, err = view.Update(ctx, bigquery.TableMetadataToUpdate{
		Schema: bqSchema,
	}, "")
	return err
}

// Load loads data from a set of GCS uris to a BigQuery table. It overwrites the existing data in
// the destination table. If the table name includes a partition decoration (e.g., table$YYYYMMDD),
// it will only overwrite said partition.
func (c *Client) Load(ctx context.Context, ds bqiface.Dataset, name string, uri ...string) error {
	gcsRef := bigquery.NewGCSReference(uri...)
	gcsRef.SourceFormat = bigquery.JSON
	tbl := ds.Table(name)
	loader := tbl.LoaderFrom(gcsRef)
	loader.SetLoadConfig(bqiface.LoadConfig{
		LoadConfig: bigquery.LoadConfig{
			Src:              gcsRef,
			WriteDisposition: bigquery.WriteTruncate,
		},
		Dst: tbl,
	})

	job, err := loader.Run(ctx)
	if err != nil {
		return err
	}

	status, err := job.Wait(ctx)
	if err != nil {
		return err
	}

	return status.Err()
}
