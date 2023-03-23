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
}

// NewClient returns a new instance of Client. Operations performed via the Client
// take place within the specified GCP `project` argument.
func NewClient(c *bigquery.Client) *Client {
	return &Client{bqiface.AdaptClient(c)}
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
	ds := c.Dataset(dt.Experiment)
	err := ds.Create(ctx, &bqiface.DatasetMetadata{
		DatasetMetadata: bigquery.DatasetMetadata{
			Name:     dt.Experiment,
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
// It returns an error if the table already exists.
func (c *Client) CreateTable(ctx context.Context, ds bqiface.Dataset, dt *api.Datatype) error {
	bqSchema, err := bigquery.SchemaFromJSON(dt.Schema)
	if err != nil {
		return err
	}

	t := ds.Table(dt.Name)
	err = t.Create(ctx, &bigquery.TableMetadata{
		Name:   dt.Name,
		Schema: bqSchema,
		TimePartitioning: &bigquery.TimePartitioning{
			Type:                   "DAY",
			Field:                  "date",
			RequirePartitionFilter: true,
		},
	})
	return err
}

// UpdateSchema updates the schema for the input `api.Datatype` table.
func (c *Client) UpdateSchema(ctx context.Context, ds bqiface.Dataset, dt *api.Datatype) error {
	bqSchema, err := bigquery.SchemaFromJSON(dt.Schema)
	if err != nil {
		return err
	}

	t := ds.Table(dt.Name)
	_, err = t.Update(ctx, bigquery.TableMetadataToUpdate{
		Schema: bqSchema,
	}, "")
	return err
}
