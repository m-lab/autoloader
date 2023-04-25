package handler

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"strings"
	"time"

	"cloud.google.com/go/bigquery"
	"github.com/googleapis/google-cloud-go-testing/bigquery/bqiface"
	"github.com/m-lab/autoloader/api"
	"github.com/m-lab/autoloader/gcs"
	"github.com/m-lab/autoloader/metrics"
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
		w.Write([]byte(err.Error()))
		return
	}

	ctx := r.Context()
	datatypes := c.GetDatatypes(ctx)
	errs := []string{}
	for _, dt := range datatypes {
		t := time.Now()
		err := c.processDatatype(ctx, dt, opts)
		if err != nil {
			metrics.AutoloadDuration.WithLabelValues(dt.Experiment, dt.Name, "error").Observe(time.Since(t).Seconds())
			errs = append(errs, fmt.Sprintf("failed to autoload %s.%s: %s", dt.Experiment, dt.Name, err.Error()))
			continue
		}
		metrics.AutoloadDuration.WithLabelValues(dt.Experiment, dt.Name, "OK").Observe(time.Since(t).Seconds())
	}

	if len(errs) != 0 {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte(strings.Join(errs, "\n")))
		return
	}

	w.WriteHeader(http.StatusOK)
}

func (c *Client) processDatatype(ctx context.Context, dt *api.Datatype, opts *LoadOptions) error {
	// Get or create dataset.
	ds, err := c.BQClient.GetDataset(ctx, dt.Dataset())
	if err != nil {
		ds, err = c.BQClient.CreateDataset(ctx, dt)
		if err != nil {
			log.Printf("failed to create BigQuery dataset %s: %v", dt.Dataset(), err)
			metrics.BigQueryOperationsTotal.WithLabelValues(dt.Experiment, dt.Name, "create-dataset", "error").Inc()
			return err
		}
		metrics.BigQueryOperationsTotal.WithLabelValues(dt.Experiment, dt.Name, "create-dataset", "OK").Inc()
	}

	// Get or create table.
	md, err := c.BQClient.GetTableMetadata(ctx, ds, dt.Table())
	if err != nil {
		md, err = c.BQClient.CreateTable(ctx, ds, dt)
		if err != nil {
			log.Printf("failed to create BigQuery table %s.%s: %v", dt.Dataset(), dt.Table(), err)
			metrics.BigQueryOperationsTotal.WithLabelValues(dt.Experiment, dt.Name, "create-table", "error").Inc()
			return err
		}
		metrics.BigQueryOperationsTotal.WithLabelValues(dt.Experiment, dt.Name, "create-table", "OK").Inc()
		// Since a new table was created, override the given optionss and default to options
		// of complete history.
		opts = periodOpts("everything")
	}

	// Update table (if necessary).
	if dt.UpdatedTime.After(md.LastModifiedTime) {
		err = c.BQClient.UpdateSchema(ctx, ds, dt)
		if err != nil {
			log.Printf("failed to update BigQuery table %s.%s: %v", dt.Dataset(), dt.Table(), err)
			metrics.BigQueryOperationsTotal.WithLabelValues(dt.Experiment, dt.Name, "update-schema", "error").Inc()
			return err
		}
		metrics.BigQueryOperationsTotal.WithLabelValues(dt.Experiment, dt.Name, "update-schema", "OK").Inc()
	}

	// Load data.
	err = c.load(ctx, ds, dt, opts)
	if err != nil {
		metrics.BigQueryOperationsTotal.WithLabelValues(dt.Experiment, dt.Name, "load", "error").Inc()
		return err
	}

	metrics.BigQueryOperationsTotal.WithLabelValues(dt.Experiment, dt.Name, "load", "OK").Inc()
	return nil
}

// load loads the contents of a set of storage directories to a date-partitioned table.
func (c *Client) load(ctx context.Context, ds bqiface.Dataset, dt *api.Datatype, opts *LoadOptions) error {
	dirs, err := c.StorageClient.GetDirs(ctx, dt, opts.start, opts.end)
	if err != nil {
		log.Printf("failed to get directories for %s.%s: %v: ", dt.Experiment, dt.Name, err)
		return err
	}

	t := time.Now()
	log.Printf("started loading data to BigQuery table %s.%s for dates %s to %s",
		dt.Dataset(), dt.Table(), opts.start, opts.end)

	for _, dir := range dirs {
		table := dt.Table() + "$" + dir.Date.Format(timex.YYYYMMDD)
		e := c.BQClient.Load(ctx, ds, table, dir.Path)
		if e != nil {
			err = e
			log.Printf("failed to load %s to BigQuery table %s: %v", dir.Path, table, e)
		}
	}

	log.Printf("finished loading data to BigQuery table %s.%s for dates %s to %s, duration: %s",
		dt.Dataset(), dt.Table(), opts.start, opts.end, time.Since(t))

	return err
}
