package handler

import (
	"context"
	"log"
	"net/http"

	"cloud.google.com/go/bigquery"
	"github.com/googleapis/google-cloud-go-testing/bigquery/bqiface"
	"github.com/m-lab/autoloader/api"
	"github.com/m-lab/autoloader/gcs"
	"github.com/m-lab/go/timex"
)

// Client contains the state needed to handle  load requests.
type Client struct {
	StorageClient
	BQClient
}

// StorageClient is an interface for types that support storage operations.
type StorageClient interface {
	GetDatatypes(context.Context) []*api.Datatype
	GetDirs(context.Context, *api.Datatype, string, string) ([]gcs.Dir, error)
}

// BQClient is an interface for types that support BigQuery operations.
type BQClient interface {
	GetDataset(context.Context, string) (bqiface.Dataset, error)
	CreateDataset(context.Context, *api.Datatype) (bqiface.Dataset, error)
	GetTableMetadata(context.Context, bqiface.Dataset, string) (*bigquery.TableMetadata, error)
	CreateTable(context.Context, bqiface.Dataset, *api.Datatype) (*bigquery.TableMetadata, error)
	UpdateSchema(context.Context, bqiface.Dataset, *api.Datatype) error
	Load(context.Context, bqiface.Dataset, string, ...string) error
}

// NewClient creates a new instance of Client.
func NewClient(storage StorageClient, bq BQClient) *Client {
	return &Client{
		StorageClient: storage,
		BQClient:      bq,
	}
}

// Load fetches the datatype information from storage and loads the archived
// data to BigQuery.
func (c *Client) Load(w http.ResponseWriter, r *http.Request) {
	opts, err := getOpts(r.URL.Query())
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	ctx := r.Context()
	datatypes := c.GetDatatypes(ctx)
	for _, dt := range datatypes {
		e := c.processDatatype(ctx, dt, opts)
		if e != nil {
			err = e
		}
	}

	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
}

func (c *Client) processDatatype(ctx context.Context, dt *api.Datatype, opts *LoadOptions) error {
	// Get or create dataset.
	ds, err := c.BQClient.GetDataset(ctx, dt.Experiment)
	if err != nil {
		ds, err = c.BQClient.CreateDataset(ctx, dt)
		if err != nil {
			log.Printf("failed to create BigQuery dataset %s: %v", dt.Experiment, err)
			return err
		}
	}

	// Get or create table.
	md, err := c.BQClient.GetTableMetadata(ctx, ds, dt.Name)
	if err != nil {
		md, err = c.BQClient.CreateTable(ctx, ds, dt)
		if err != nil {
			log.Printf("failed to create BigQuery table %s.%s: %v", dt.Experiment, dt.Name, err)
			return err
		}
		opts = periodOpts("new")
	}

	// Update table (if necessary).
	if dt.UpdatedTime.After(md.LastModifiedTime) {
		err = c.BQClient.UpdateSchema(ctx, ds, dt)
		if err != nil {
			log.Printf("failed to update BigQuery table %s.%s: %v", dt.Experiment, dt.Name, err)
			return err
		}
	}

	// Load data.
	return c.load(ctx, ds, dt, opts)
}

// load loads the contents of a set of storage directories to a date-partitioned table.
func (c *Client) load(ctx context.Context, ds bqiface.Dataset, dt *api.Datatype, opts *LoadOptions) error {
	dirs, err := c.StorageClient.GetDirs(ctx, dt, opts.start, opts.end)
	if err != nil {
		log.Printf("failed to get directories for %s.%s: %v: ", dt.Experiment, dt.Name, err)
		return err
	}

	for _, dir := range dirs {
		table := dt.Name + "$" + dir.Date.Format(timex.YYYYMMDD)
		e := c.BQClient.Load(ctx, ds, table, dir.Path)
		if e != nil {
			err = e
			log.Printf("failed to load %s to BigQuery table %s: %v", dir.Path, table, e)
		}
	}

	return err
}
