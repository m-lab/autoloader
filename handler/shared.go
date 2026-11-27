package handler

import (
	"context"
	"log"
	"regexp"
	"sort"
	"time"

	"cloud.google.com/go/bigquery"
	"github.com/googleapis/google-cloud-go-testing/bigquery/bqiface"
	"github.com/m-lab/autoloader/api"
	"github.com/m-lab/autoloader/gcs"
	"github.com/m-lab/autoloader/metrics"
	"github.com/m-lab/go/timex"
)

// changeMargin is subtracted from a partition's last-modified time when
// deciding whether a date has new objects. It covers an object that arrives
// while a load of its date is running: such an object is older than the
// partition's new last-modified time and would otherwise never be noticed.
const changeMargin = 6 * time.Hour

// loadShared loads a datatype whose orgs share one table. Each date with new
// objects is loaded with a single hive-partitioned job that names every org's
// directory. A failed date does not stop later dates; the last error is
// returned.
func (c *Client) loadShared(ctx context.Context, ds bqiface.Dataset, dt *api.Datatype, opts *LoadOptions) error {
	dirs, err := c.StorageClient.GetDirs(ctx, dt, opts.start, opts.end)
	if err != nil {
		log.Printf("failed to get directories for %s.%s: %v", dt.Experiment, dt.Name, err)
		return err
	}

	// Group the orgs' directories by date.
	byDate := map[string][]gcs.Dir{}
	for _, dir := range dirs {
		if dir.Date.IsZero() {
			// The path looks like a date but is not one (e.g., 2026/11/31).
			log.Printf("skipping directory without a valid date: %s", dir.Path)
			continue
		}
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
			metrics.LoadedDates.WithLabelValues(Version(dt), dt.Experiment, dt.Name, opts.period, "error").Set(unix)
			continue
		}
		metrics.LoadedDates.WithLabelValues(Version(dt), dt.Experiment, dt.Name, opts.period, "OK").Set(unix)
	}

	log.Printf("finished loading data to BigQuery table %s.%s for dates %s to %s, loaded %d of %d dates, duration: %s",
		dt.Dataset(), dt.Table(), opts.start, opts.end, loaded, len(dates), time.Since(t))

	return err
}

// changed reports whether a date has to be loaded: always for an explicit
// date range, and otherwise when its partition is empty, when its metadata
// cannot be read, or when an object is newer than the partition's last load
// (less changeMargin).
func (c *Client) changed(ctx context.Context, ds bqiface.Dataset, partition string, group []gcs.Dir, opts *LoadOptions) bool {
	if opts.period == "custom" {
		return true
	}

	md, err := c.BQClient.GetTableMetadata(ctx, ds, partition)
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
func (c *Client) loadDate(ctx context.Context, ds bqiface.Dataset, dt *api.Datatype, date string, group []gcs.Dir) error {
	partition := dt.Table() + "$" + date
	paths := make([]string, 0, len(group))
	for _, dir := range group {
		paths = append(paths, dir.Path)
	}

	err := c.BQClient.LoadHive(ctx, ds, partition, dt, paths...)
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

// countLoadErrors increments metrics.LoadErrorsTotal for a failed shared load.
// BigQuery names the offending file for most bad data (corrupt or malformed
// files, unknown fields), which identifies the org: each org named is counted
// once per reason. An error that names no file, such as a backend error or a
// row whose date differs from the partition's, is counted once under the org
// "unknown".
func countLoadErrors(dt *api.Datatype, err error) {
	for _, f := range loadFailures(err) {
		metrics.LoadErrorsTotal.WithLabelValues(Version(dt), dt.Experiment, dt.Name, f.org, f.reason).Inc()
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
