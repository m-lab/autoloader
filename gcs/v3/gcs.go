package v3

import (
	"context"
	"fmt"
	"io"
	"log"
	"path"
	"regexp"
	"strings"
	"time"

	"cloud.google.com/go/storage"
	"github.com/m-lab/autoloader/api"
	apiv3 "github.com/m-lab/autoloader/api/v3"
	"github.com/m-lab/go/storagex"
	"github.com/m-lab/go/timex"
	"google.golang.org/api/iterator"
)

var (
	// orgPrefix matches a top-level "org=<name>" prefix and captures the name.
	orgPrefix = regexp.MustCompile(`^org=([a-z0-9]([a-z0-9-]*[a-z0-9])?)$`)
	// dateDir matches the "YYYY/MM/DD/" start of an object name relative to
	// its datatype directory and captures the date.
	dateDir = regexp.MustCompile(`^(\d{4}/\d{2}/\d{2})/`)
)

// Dir is the directory of one organization's data for one datatype and date.
type Dir struct {
	Path   string    // GCS path (e.g., "gs://bucket/autoload/v3/org=foo/ndt/ndt7/2026/11/18/*").
	Date   time.Time // Path date.
	Org    string    // Organization that owns the directory.
	Newest time.Time // Creation time of the newest object in the directory.
}

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
func (c *Client) GetDatatypes(ctx context.Context) []*apiv3.Datatype {
	datatypes := make([]*apiv3.Datatype, 0)
	// Destination tables seen so far and the schema object that claimed them.
	seen := map[string]string{}

	for _, bucket := range c.Buckets {
		err := bucket.Walk(ctx, path.Join(prefix, "tables"), func(schema *storagex.Object) error {
			dt, err := getDatatype(ctx, bucket, schema)
			if err != nil {
				log.Printf("skipping v3 schema: %v", err)
				return err
			}

			// The destination table has no bucket in its name. Two buckets
			// with the same experiment and datatype would overwrite each
			// other's partitions, so only the first one is loaded.
			table := dt.Dataset() + "." + dt.Table()
			source := "gs://" + schema.Bucket + "/" + schema.Name
			if first, ok := seen[table]; ok {
				err := fmt.Errorf("%s is already loaded from %s", table, first)
				log.Printf("skipping v3 schema %s: %v", source, err)
				return err
			}
			seen[table] = source

			datatypes = append(datatypes, dt)
			return nil
		})
		if err != nil {
			log.Printf("failed to list v3 schemas: %v", err)
		}
	}

	return datatypes
}

// getDatatype creates the datatype for a schema object.
func getDatatype(ctx context.Context, b *storagex.Bucket, schema *storagex.Object) (*apiv3.Datatype, error) {
	// Expect "autoload/v3/tables/<experiment>/<datatype>.table.json".
	parts := strings.Split(schema.Name, "/")
	if len(parts) != 5 || parts[2] != "tables" || !strings.HasSuffix(parts[4], schemaFileSuffix) {
		return nil, fmt.Errorf("invalid schema path %s", schema.Name)
	}

	file, err := readFile(ctx, schema)
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
		UpdatedTime: schema.Updated,
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

// readFile returns the contents of an object.
func readFile(ctx context.Context, o *storagex.Object) ([]byte, error) {
	reader, err := o.NewReader(ctx)
	if err != nil {
		return nil, err
	}
	defer reader.Close()
	return io.ReadAll(reader)
}

// GetDirs returns every organization's directories for a datatype within a
// start (inclusive) and end (exclusive) date, ordered by organization and then
// by date. Organizations without objects in the date range contribute nothing.
func (c *Client) GetDirs(ctx context.Context, dt *apiv3.Datatype, start, end string) ([]Dir, error) {
	orgs, err := getBucketOrgs(ctx, dt.Bucket)
	if err != nil {
		log.Printf("failed to get organizations for bucket: %v", err)
		return nil, err
	}

	var dirs []Dir
	for _, org := range orgs {
		orgDirs, err := getOrgDirs(ctx, dt.Bucket, org, path.Join(prefix, "org="+org, dt.Experiment, dt.Name), start, end)
		if err != nil {
			log.Printf("failed to list bucket: %v", err)
			return nil, err
		}
		dirs = append(dirs, orgDirs...)
	}

	return dirs, nil
}

// getOrgDirs returns the date directories "<p>/YYYY/MM/DD" of one organization
// within a start (inclusive) and end (exclusive) date, each with the creation
// time of its newest object.
func getOrgDirs(ctx context.Context, b *storagex.Bucket, org, p, start, end string) ([]Dir, error) {
	it := b.Objects(ctx, &storage.Query{
		Prefix:      p + "/",
		StartOffset: path.Join(p, start),
		EndOffset:   path.Join(p, end),
	})

	var dirs []Dir
	index := map[string]int{} // Date to position in dirs.
	for {
		attr, err := it.Next()
		if err == iterator.Done {
			return dirs, nil
		}
		if err != nil {
			return nil, err
		}

		m := dateDir.FindStringSubmatch(strings.TrimPrefix(attr.Name, p+"/"))
		if m == nil {
			continue
		}
		if i, ok := index[m[1]]; ok {
			if attr.Created.After(dirs[i].Newest) {
				dirs[i].Newest = attr.Created
			}
			continue
		}

		date, err := time.Parse(timex.YYYYMMDDWithSlash, m[1])
		if err != nil {
			// The path looks like a date but is not one (e.g., 2026/11/31).
			log.Printf("skipping object without a valid date: %s", attr.Name)
			continue
		}
		index[m[1]] = len(dirs)
		dirs = append(dirs, Dir{
			Path:   "gs://" + path.Join(attr.Bucket, p, m[1], "*"),
			Date:   date,
			Org:    org,
			Newest: attr.Created,
		})
	}
}
