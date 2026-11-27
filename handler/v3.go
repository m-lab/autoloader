package handler

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"regexp"
	"sort"
	"strings"
	"time"

	"cloud.google.com/go/bigquery"
	"github.com/googleapis/google-cloud-go-testing/bigquery/bqiface"
	"github.com/m-lab/autoloader/api"
	apiv3 "github.com/m-lab/autoloader/api/v3"
	gcsv3 "github.com/m-lab/autoloader/gcs/v3"
	"github.com/m-lab/autoloader/metrics"
	"github.com/m-lab/go/timex"
)

// This file holds the handler of v3 load requests. All organizations of a v3
// datatype are loaded into one table, so it is discovered, created, updated
// and loaded differently from v1 and v2 datatypes, which Client handles.

// ClientV3 contains the state needed to handle v3 load requests.
type ClientV3 struct {
	storage StorageClientV3
	bq      BQClientV3
}

// StorageClientV3 is an interface for types that support the storage
// operations of v3 datatypes.
type StorageClientV3 interface {
	GetDatatypes(context.Context) []*apiv3.Datatype
	GetDirs(context.Context, *apiv3.Datatype, string, string) ([]gcsv3.Dir, error)
}

// BQClientV3 is an interface for types that support the BigQuery
// operations of v3 datatypes.
type BQClientV3 interface {
	GetDataset(context.Context, string) (bqiface.Dataset, error)
	CreateDataset(context.Context, *api.Datatype) (bqiface.Dataset, error)
	GetTableMetadata(context.Context, bqiface.Dataset, string) (*bigquery.TableMetadata, error)
	CreateTableV3(context.Context, bqiface.Dataset, *apiv3.Datatype) (*bigquery.TableMetadata, error)
	UpdateSchemaV3(context.Context, bqiface.Dataset, *apiv3.Datatype) error
	LoadHive(context.Context, bqiface.Dataset, string, *apiv3.Datatype, ...string) error
}

// NewClientV3 creates a new instance of ClientV3.
func NewClientV3(storage StorageClientV3, bq BQClientV3) *ClientV3 {
	return &ClientV3{
		storage: storage,
		bq:      bq,
	}
}

// Load fetches the v3 datatypes from storage and loads the archived data of
// all their organizations to BigQuery.
func (c *ClientV3) Load(w http.ResponseWriter, r *http.Request) {
	opts, err := getOpts(r.URL.Query())
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte(err.Error()))
		return
	}

	ctx := r.Context()
	errs := []string{}
	for _, dt := range c.storage.GetDatatypes(ctx) {
		t := time.Now()
		status := "OK"
		if err := c.processDatatype(ctx, dt, opts); err != nil {
			status = "error"
			errs = append(errs, fmt.Sprintf("failed to autoload %s.%s: %s", dt.Experiment, dt.Name, err.Error()))
		}
		metrics.AutoloadDuration.WithLabelValues(dt.Version, dt.Experiment, dt.Name, opts.period, status).Observe(time.Since(t).Seconds())
	}

	if len(errs) != 0 {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte(strings.Join(errs, "\n")))
		return
	}

	w.WriteHeader(http.StatusOK)
}

// processDatatype gets or creates the dataset and the table of a
// datatype, updates the table's schema and loads the data.
func (c *ClientV3) processDatatype(ctx context.Context, dt *apiv3.Datatype, opts *LoadOptions) error {
	count := func(operation string, err error) {
		status := "OK"
		if err != nil {
			status = "error"
		}
		metrics.BigQueryOperationsTotal.WithLabelValues(dt.Version, dt.Experiment, dt.Name, operation, status).Inc()
	}

	// Get or create dataset.
	ds, err := c.bq.GetDataset(ctx, dt.Dataset())
	if err != nil {
		ds, err = c.bq.CreateDataset(ctx, dt.Datatype)
		count("create-dataset", err)
		if err != nil {
			log.Printf("failed to create BigQuery dataset %s: %v", dt.Dataset(), err)
			return err
		}
	}

	// Get or create table.
	_, err = c.bq.GetTableMetadata(ctx, ds, dt.Table())
	if err != nil {
		_, err = c.bq.CreateTableV3(ctx, ds, dt)
		count("create-table", err)
		if err != nil {
			log.Printf("failed to create BigQuery table %s.%s: %v", dt.Dataset(), dt.Table(), err)
			return err
		}
		// A new table is loaded with the complete history.
		opts = periodOpts("everything")
	}

	// Update the schema on every run. Comparing the schema file's time with
	// the table's, as v1 and v2 do, misses a file uploaded while a run is in
	// progress: by the next run it is older than the table.
	err = c.bq.UpdateSchemaV3(ctx, ds, dt)
	count("update-schema", err)
	if err != nil {
		log.Printf("failed to update BigQuery table %s.%s: %v", dt.Dataset(), dt.Table(), err)
		return err
	}

	// Load data.
	err = c.load(ctx, ds, dt, opts)
	count("load", err)
	return err
}

// changeMargin is subtracted from a partition's last-modified time when
// deciding whether a date has new objects. It covers an object that arrives
// while a load of its date is running: such an object is older than the
// partition's new last-modified time and would otherwise never be noticed.
const changeMargin = 6 * time.Hour

// load loads the data of a datatype. Each date with new objects is loaded with
// a single hive-partitioned job that names every org's directory. A failed
// date does not stop later dates; the last error is returned.
func (c *ClientV3) load(ctx context.Context, ds bqiface.Dataset, dt *apiv3.Datatype, opts *LoadOptions) error {
	dirs, err := c.storage.GetDirs(ctx, dt, opts.start, opts.end)
	if err != nil {
		log.Printf("failed to get directories for %s.%s: %v", dt.Experiment, dt.Name, err)
		return err
	}

	// Group the orgs' directories by date.
	byDate := map[string][]gcsv3.Dir{}
	for _, dir := range dirs {
		d := dir.Date.Format(timex.YYYYMMDD)
		byDate[d] = append(byDate[d], dir)
	}
	dates := make([]string, 0, len(byDate))
	for d := range byDate {
		dates = append(dates, d)
	}
	sort.Strings(dates)

	t := time.Now()
	log.Printf("started loading data to BigQuery table %s.%s for dates %s to %s",
		dt.Dataset(), dt.Table(), opts.start, opts.end)

	loaded := 0
	for _, date := range dates {
		if ctx.Err() != nil {
			// The request was cancelled: every remaining call would fail.
			err = ctx.Err()
			break
		}
		group := byDate[date]
		sort.SliceStable(group, func(i, j int) bool { return group[i].Org < group[j].Org })
		partition := dt.Table() + "$" + date

		if !c.changed(ctx, ds, partition, group, opts) {
			continue
		}
		loaded++

		unix := float64(group[0].Date.Unix())
		e := c.loadDate(ctx, ds, dt, date, group)
		if e != nil {
			err = e
			log.Printf("failed to load %s to BigQuery table %s.%s: %v", date, dt.Dataset(), partition, e)
			metrics.LoadedDates.WithLabelValues(dt.Version, dt.Experiment, dt.Name, opts.period, "error").Set(unix)
			continue
		}
		metrics.LoadedDates.WithLabelValues(dt.Version, dt.Experiment, dt.Name, opts.period, "OK").Set(unix)
	}

	log.Printf("finished loading data to BigQuery table %s.%s for dates %s to %s, loaded %d of %d dates, duration: %s",
		dt.Dataset(), dt.Table(), opts.start, opts.end, loaded, len(dates), time.Since(t))

	return err
}

// changed reports whether a date has to be loaded: always for an explicit
// date range, and otherwise when its partition is empty, when its metadata
// cannot be read, or when an object is newer than the partition's last load
// (less changeMargin).
func (c *ClientV3) changed(ctx context.Context, ds bqiface.Dataset, partition string, group []gcsv3.Dir, opts *LoadOptions) bool {
	if opts.period == "custom" {
		return true
	}

	md, err := c.bq.GetTableMetadata(ctx, ds, partition)
	if err != nil {
		log.Printf("failed to get metadata of %s, loading it: %v", partition, err)
		return true
	}
	if md.NumRows == 0 {
		return true
	}

	var newest time.Time
	for _, dir := range group {
		if dir.Newest.After(newest) {
			newest = dir.Newest
		}
	}
	return newest.After(md.LastModifiedTime.Add(-changeMargin))
}

// loadDate loads every org's directory for one date with a single job. If
// the job fails, the destination partition is left as it was, so no org gets
// that date's new data until the cause is gone, and the failure is counted
// per org (see countLoadErrors).
func (c *ClientV3) loadDate(ctx context.Context, ds bqiface.Dataset, dt *apiv3.Datatype, date string, group []gcsv3.Dir) error {
	partition := dt.Table() + "$" + date
	paths := make([]string, 0, len(group))
	for _, dir := range group {
		paths = append(paths, dir.Path)
	}

	err := c.bq.LoadHive(ctx, ds, partition, dt, paths...)
	if err != nil && ctx.Err() == nil {
		// A cancelled request says nothing about the data.
		countLoadErrors(dt, err)
	}
	return err
}

// orgInPath captures the org of an object path under a hive prefix.
var orgInPath = regexp.MustCompile(`/org=([^/\s]+)/`)

// unknown is the label value for an org or a reason that an error does not
// name.
const unknown = "unknown"

// loadFailure is one org and BigQuery reason to count for a failed load.
type loadFailure struct {
	org    string
	reason string
}

// countLoadErrors increments metrics.LoadErrorsTotal for a failed load.
// BigQuery names the offending file for most bad data (corrupt or malformed
// files, unknown fields), which identifies the org: each org named is counted
// once per reason. An error that names no file, such as a backend error or a
// row whose date differs from the partition's, is counted once under the org
// "unknown".
func countLoadErrors(dt *apiv3.Datatype, err error) {
	for _, f := range loadFailures(err) {
		metrics.LoadErrorsTotal.WithLabelValues(dt.Version, dt.Experiment, dt.Name, f.org, f.reason).Inc()
	}
}

// loadFailures returns the distinct orgs and reasons to count for err, in the
// order they appear.
func loadFailures(err error) []loadFailure {
	var named, unnamed []loadFailure
	seen := map[loadFailure]bool{}
	for _, e := range bqErrors(err) {
		reason := e.Reason
		if reason == "" {
			reason = unknown
		}
		m := orgInPath.FindStringSubmatch(e.Location + " " + e.Message)
		if m == nil {
			unnamed = append(unnamed, loadFailure{unknown, reason})
			continue
		}
		f := loadFailure{m[1], reason}
		if !seen[f] {
			seen[f] = true
			named = append(named, f)
		}
	}

	switch {
	case len(named) != 0:
		return named
	case len(unnamed) != 0:
		return unnamed[:1]
	default:
		// Not a BigQuery job error (e.g., the job could not be started).
		return []loadFailure{{unknown, unknown}}
	}
}

// bqErrors returns the BigQuery errors wrapped or joined in err.
func bqErrors(err error) []*bigquery.Error {
	var errs []*bigquery.Error
	var walk func(error)
	walk = func(e error) {
		switch e := e.(type) {
		case nil:
		case *bigquery.Error:
			errs = append(errs, e)
		case interface{ Unwrap() []error }:
			for _, u := range e.Unwrap() {
				walk(u)
			}
		case interface{ Unwrap() error }:
			walk(e.Unwrap())
		}
	}
	walk(err)
	return errs
}
