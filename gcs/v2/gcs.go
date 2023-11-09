package v2

import (
	"context"
	"fmt"
	"path"
	"strings"

	"cloud.google.com/go/storage"
	"github.com/m-lab/autoloader/api"
	apiv2 "github.com/m-lab/autoloader/api/v2"
	"github.com/m-lab/autoloader/gcs"
	"github.com/m-lab/go/storagex"
	"google.golang.org/api/iterator"
)

var (
	datePattern = `/\d{4}/[01]\d/[0123]\d`
)

const (
	prefix           = "autoload/v2/"
	schemaFileSuffix = ".table.json"
)

// ClientV2 is the V2 client used to interact with Google Cloud Storage.
type ClientV2 struct {
	Buckets []*storagex.Bucket
}

// BucketV2 represents a V2 GCS bucket.
type BucketV2 struct {
	*storagex.Bucket
	orgs []string
}

// NewClient returns a new Client for the specified bucket names.
func NewClient(c *storage.Client, names []string) *ClientV2 {
	buckets := make([]*storagex.Bucket, 0)
	for _, name := range names {
		bh := c.Bucket(name)
		buckets = append(buckets, storagex.NewBucket(bh))
	}

	return &ClientV2{
		Buckets: buckets,
	}
}

// GetDatatypes gets a list of datatypes for all buckets.
func (c *ClientV2) GetDatatypes(ctx context.Context) []*api.Datatype {
	datatypes := make([]*api.Datatype, 0)

	for _, bucket := range c.Buckets {
		bktOrgs, err := getBucketOrgs(ctx, bucket)
		if err != nil {
			continue
		}
		b := &BucketV2{Bucket: bucket, orgs: bktOrgs}

		b.Walk(ctx, path.Join(prefix, "tables"), func(schema *storagex.Object) error {
			dts, err := getDatatypes(ctx, b, schema)
			if err != nil {
				return err
			}

			datatypes = append(datatypes, dts...)
			return nil
		})
	}

	return datatypes
}

func getBucketOrgs(ctx context.Context, b *storagex.Bucket) ([]string, error) {
	orgs := make([]string, 0)

	it := b.Objects(ctx, &storage.Query{
		Prefix:    prefix,
		Delimiter: "/",
	})

	for {
		attr, err := it.Next()
		if err == iterator.Done {
			return orgs, nil
		}

		if err != nil {
			return nil, err
		}

		if attr.Prefix == "" {
			continue
		}

		parts := strings.Split(attr.Prefix, "/")
		if len(parts) != 4 || parts[2] == "tables" {
			continue
		}
		orgs = append(orgs, parts[2])
	}
}

func getDatatypes(ctx context.Context, b *BucketV2, schema *storagex.Object) ([]*api.Datatype, error) {
	file, err := gcs.ReadFile(ctx, schema.ObjectHandle)
	if err != nil || len(file) == 0 {
		return nil, fmt.Errorf("invalid schema file under %s", schema.Name)
	}

	attrs, err := b.Attrs(ctx)
	if err != nil {
		return nil, err
	}

	path, err := NewSchemaPath(ctx, b, schema.Name)
	if err != nil {
		return nil, err
	}

	dts := make([]*api.Datatype, 0)
	for _, org := range path.Organizations {
		opts := api.DatatypeOpts{
			Name:         path.Datatype,
			Experiment:   path.Experiment,
			Organization: org,
			Version:      "v2",
			Location:     attrs.Location,
			Schema:       file,
			UpdatedTime:  schema.ObjectAttrs.Updated,
			Bucket:       b.Bucket,
		}
		dts = append(dts, getDatatype(schema.Bucket, opts))
	}

	return dts, nil
}

func getDatatype(bucketName string, opts api.DatatypeOpts) *api.Datatype {
	proj := strings.TrimPrefix(bucketName, "archive-")
	switch proj {
	case "mlab-autojoin":
		fallthrough
	case "mlab-thirdparty":
		return apiv2.NewBYODatatype(opts, proj)
	default:
		return apiv2.NewMlabDatatype(opts)
	}
}

// GetDirs returns all the directory paths for a datatype within a start (inclusive) and
// end (exclusive) date.
func (c *ClientV2) GetDirs(ctx context.Context, dt *api.Datatype, start, end string) ([]gcs.Dir, error) {
	p := path.Join(prefix, dt.Organization, dt.Experiment, dt.Name)
	return gcs.GetDirs(ctx, dt, p, start, end)
}
