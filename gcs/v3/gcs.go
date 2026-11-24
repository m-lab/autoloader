package v3

import (
	"context"
	"fmt"
	"log"
	"path"
	"regexp"
	"strings"

	"cloud.google.com/go/storage"
	"github.com/m-lab/autoloader/api"
	apiv3 "github.com/m-lab/autoloader/api/v3"
	"github.com/m-lab/autoloader/gcs"
	"github.com/m-lab/go/storagex"
	"google.golang.org/api/iterator"
)

var (
	// orgPrefix matches a top-level "org=<name>" prefix and captures the name.
	orgPrefix = regexp.MustCompile(`^org=([a-z0-9]([a-z0-9-]*[a-z0-9])?)$`)
)

const (
	prefix           = "autoload/v3/"
	schemaFileSuffix = ".table.json"
)

// Client is the v3 client used to interact with Google Cloud Storage.
type Client struct {
	Buckets []*storagex.Bucket
}

// NewClient returns a new Client for the specified bucket names.
func NewClient(c *storage.Client, names []string) *Client {
	buckets := make([]*storagex.Bucket, 0)
	for _, name := range names {
		bh := c.Bucket(name)
		buckets = append(buckets, storagex.NewBucket(bh))
	}

	return &Client{
		Buckets: buckets,
	}
}

// GetDatatypes gets a list of datatypes for the Client's buckets. A datatype is
// denoted by a schema file "autoload/v3/tables/<experiment>/<datatype>.table.json".
func (c *Client) GetDatatypes(ctx context.Context) []*api.Datatype {
	datatypes := make([]*api.Datatype, 0)

	for _, bucket := range c.Buckets {
		bucket.Walk(ctx, path.Join(prefix, "tables"), func(schema *storagex.Object) error {
			dt, err := getDatatype(ctx, bucket, schema)
			if err != nil {
				log.Printf("skipping v3 schema: %v", err)
				return err
			}

			datatypes = append(datatypes, dt)
			return nil
		})
	}

	return datatypes
}

// getDatatype creates the datatype for a schema object.
func getDatatype(ctx context.Context, b *storagex.Bucket, schema *storagex.Object) (*api.Datatype, error) {
	// Expect "autoload/v3/tables/<experiment>/<datatype>.table.json".
	parts := strings.Split(schema.Name, "/")
	if len(parts) != 5 || parts[2] != "tables" || !strings.HasSuffix(parts[4], schemaFileSuffix) {
		return nil, fmt.Errorf("invalid schema path %s", schema.Name)
	}

	file, err := gcs.ReadFile(ctx, schema.ObjectHandle)
	if err != nil || len(file) == 0 {
		return nil, fmt.Errorf("invalid schema file under %s", schema.Name)
	}

	attrs, err := b.Attrs(ctx)
	if err != nil {
		return nil, err
	}

	opts := api.DatatypeOpts{
		Name:        strings.TrimSuffix(parts[4], schemaFileSuffix),
		Experiment:  parts[3],
		Version:     "v3",
		Location:    attrs.Location,
		Schema:      file,
		UpdatedTime: schema.ObjectAttrs.Updated,
		Bucket:      b,
	}

	return apiv3.NewDatatype(opts, "gs://"+schema.Bucket+"/"+strings.TrimSuffix(prefix, "/")), nil
}

// getBucketOrgs gets the list of organizations uploading data to a bucket, in
// lexicographic order.
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

		parts := strings.Split(attr.Prefix, "/")
		if len(parts) != 4 {
			continue
		}
		m := orgPrefix.FindStringSubmatch(parts[2])
		if m == nil {
			continue
		}
		orgs = append(orgs, m[1])
	}
}

// GetDirs returns all the directory paths for a datatype within a start (inclusive) and
// end (exclusive) date, tagged with their organization. Organizations without objects
// in the date range contribute nothing.
func (c *Client) GetDirs(ctx context.Context, dt *api.Datatype, start, end string) ([]gcs.Dir, error) {
	orgs, err := getBucketOrgs(ctx, dt.Bucket)
	if err != nil {
		log.Printf("failed to get organizations for bucket: %v", err)
		return nil, err
	}

	var dirs []gcs.Dir
	for _, org := range orgs {
		p := path.Join(prefix, "org="+org, dt.Experiment, dt.Name)
		orgDirs, err := gcs.GetDirs(ctx, dt, p, start, end)
		if err != nil {
			return nil, err
		}

		for _, d := range orgDirs {
			d.Org = org
			dirs = append(dirs, d)
		}
	}

	return dirs, nil
}
