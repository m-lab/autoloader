package bq

import (
	"context"
	"errors"
	"strings"
	"time"

	"cloud.google.com/go/bigquery"
	"github.com/googleapis/google-cloud-go-testing/bigquery/bqiface"
	"github.com/m-lab/autoloader/api"
)

// stagingTableExpiration is the default expiration of the tables in a
// staging dataset.
const stagingTableExpiration = 48 * time.Hour

// errNoSources is returned by CopyPartitions when it has no source tables,
// so that a partition is never replaced by nothing.
var errNoSources = errors.New("copy job needs at least one source table")

// StagingDataset returns the name of the dataset that holds the per-org
// staging tables of a shared-table datatype (e.g., "autoload_v3_ndt_staging").
func StagingDataset(dt *api.Datatype) string {
	return dt.Dataset() + "_staging"
}

// StagingTable returns the name of the staging table for one org of a
// shared-table datatype (e.g., "ndt7_foo_bar" for org "foo-bar").
func StagingTable(dt *api.Datatype, org string) string {
	return dt.Name + "_" + strings.ReplaceAll(org, "-", "_")
}

// EnsureStagingDataset returns the staging dataset of the input datatype,
// creating it if it does not exist. Tables in it expire after 48 hours by
// default.
func (c *Client) EnsureStagingDataset(ctx context.Context, dt *api.Datatype) (bqiface.Dataset, error) {
	name := StagingDataset(dt)
	ds := c.Dataset(name)
	if _, err := ds.Metadata(ctx); err == nil {
		return ds, nil
	}
	err := ds.Create(ctx, &bqiface.DatasetMetadata{
		DatasetMetadata: bigquery.DatasetMetadata{
			Name: name,
			// TODO(github.com/m-lab/gcp-config/issues/73): Fix bucket location.
			Location:               "US",
			DefaultTableExpiration: stagingTableExpiration,
		},
	})
	if err != nil {
		// Another run may have created it in the meantime.
		if _, e := ds.Metadata(ctx); e != nil {
			return nil, err
		}
	}
	return ds, nil
}

// EnsureStagingTable creates the staging table of the input org in the
// staging dataset ds, with the same schema, partitioning and clustering as
// the datatype's shared table, as a copy job requires. If the table already
// exists, its schema is updated.
func (c *Client) EnsureStagingTable(ctx context.Context, ds bqiface.Dataset, dt *api.Datatype, org string) error {
	name := StagingTable(dt, org)
	md, err := tableMetadata(dt, name)
	if err != nil {
		return err
	}
	t := ds.Table(name)
	if _, err := t.Metadata(ctx); err != nil {
		err = t.Create(ctx, md)
		if err == nil {
			return nil
		}
		// Another run may have created it in the meantime.
		if _, e := t.Metadata(ctx); e != nil {
			return err
		}
	}
	_, err = t.Update(ctx, bigquery.TableMetadataToUpdate{
		Schema: md.Schema,
	}, "")
	return err
}

// CopyPartitions runs one copy job that overwrites dst with the contents of
// srcs and waits for it. Both are expected to name day partitions (e.g.,
// table$YYYYMMDD). The destination must exist. With zero sources it returns
// an error without running a job.
func (c *Client) CopyPartitions(ctx context.Context, dst bqiface.Table, srcs ...bqiface.Table) error {
	if len(srcs) == 0 {
		return errNoSources
	}
	copier := dst.CopierFrom(srcs...)
	copier.SetCopyConfig(bqiface.CopyConfig{
		CopyConfig: bigquery.CopyConfig{
			CreateDisposition: bigquery.CreateNever,
			WriteDisposition:  bigquery.WriteTruncate,
		},
		Srcs: srcs,
		Dst:  dst,
	})

	job, err := copier.Run(ctx)
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
