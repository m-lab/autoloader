package handler

import (
	"context"
	"errors"
	"fmt"
	"log"
	"sort"
	"time"

	"github.com/googleapis/google-cloud-go-testing/bigquery/bqiface"
	"github.com/m-lab/autoloader/api"
	"github.com/m-lab/autoloader/bq"
	"github.com/m-lab/autoloader/gcs"
	"github.com/m-lab/autoloader/metrics"
	"github.com/m-lab/go/timex"
)

// changeMargin is subtracted from a partition's last-modified time when
// deciding whether a date has new objects. It covers an object that arrives
// while a load of its date is running: such an object is older than the
// partition's new last-modified time and would otherwise never be noticed.
const changeMargin = 6 * time.Hour

// errNoOrgLoaded is returned when the per-org fallback could not load any org
// of a date.
var errNoOrgLoaded = errors.New("no org could be loaded")

// loadShared loads a datatype whose orgs share one table. Each date with new
// objects is loaded with a single hive-partitioned job that names every org's
// directory. If that job fails, the date is loaded org by org (see loadByOrg).
// A failed date does not stop later dates; the last error is returned.
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

// loadDate loads every org's directory for one date with a single job, and
// falls back to loading org by org if that job fails for any reason.
func (c *Client) loadDate(ctx context.Context, ds bqiface.Dataset, dt *api.Datatype, date string, group []gcs.Dir) error {
	partition := dt.Table() + "$" + date
	paths := make([]string, 0, len(group))
	for _, dir := range group {
		paths = append(paths, dir.Path)
	}

	err := c.BQClient.LoadHive(ctx, ds, partition, dt, paths...)
	if err == nil {
		return nil
	}
	log.Printf("failed to load %d orgs to BigQuery table %s.%s, loading org by org: %v",
		len(group), dt.Dataset(), partition, err)

	return c.loadByOrg(ctx, ds, dt, date, group)
}

// loadByOrg loads each org's directory for one date into its own staging
// table and then replaces the destination partition with a copy of the
// staging partitions of the orgs that loaded. An org that fails is excluded,
// so the partition afterwards holds the other orgs only, and an error is
// returned. If no org loads, the copy fails or the request is cancelled, the
// destination partition is left untouched and no org counts as excluded.
func (c *Client) loadByOrg(ctx context.Context, ds bqiface.Dataset, dt *api.Datatype, date string, group []gcs.Dir) error {
	partition := dt.Table() + "$" + date

	stg, err := c.BQClient.EnsureStagingDataset(ctx, dt)
	if err != nil {
		return fmt.Errorf("failed to get staging dataset %s: %w", bq.StagingDataset(dt), err)
	}

	var srcs []bqiface.Table
	var excluded []string
	for _, dir := range group {
		name := bq.StagingTable(dt, dir.Org) + "$" + date
		err := c.BQClient.EnsureStagingTable(ctx, stg, dt, dir.Org)
		if err == nil {
			err = c.BQClient.LoadHive(ctx, stg, name, dt, dir.Path)
		}
		if err != nil {
			if ctx.Err() != nil {
				// The request was cancelled, which says nothing about the org.
				return ctx.Err()
			}
			log.Printf("failed to load org %s for BigQuery table %s.%s: %s to staging table %s: %v",
				dir.Org, dt.Dataset(), partition, dir.Path, name, err)
			excluded = append(excluded, dir.Org)
			continue
		}
		srcs = append(srcs, stg.Table(name))
	}

	if len(srcs) == 0 {
		return errNoOrgLoaded
	}

	err = c.BQClient.CopyPartitions(ctx, ds.Table(partition), srcs...)
	if err != nil {
		return fmt.Errorf("failed to copy staging partitions: %w", err)
	}

	// Only now is an org really excluded: the partition holds the other orgs.
	for _, org := range excluded {
		log.Printf("excluded org %s from BigQuery table %s.%s", org, dt.Dataset(), partition)
		metrics.ExcludedOrgsTotal.WithLabelValues(Version(dt), dt.Experiment, dt.Name, org).Inc()
	}
	if len(excluded) != 0 {
		return fmt.Errorf("loaded without excluded orgs %v", excluded)
	}
	return nil
}
