package gcs

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
	set "github.com/deckarep/golang-set/v2"
	"github.com/m-lab/autoloader/api"
	"github.com/m-lab/go/storagex"
	"github.com/m-lab/go/timex"
	"google.golang.org/api/iterator"
)

var (
	datePattern = `/\d{4}/[01]\d/[0123]\d`
)

const (
	prefix           = "autoload/v1/"
	schemaFileSuffix = ".table.json"
)

// Client is used to interact with Google Cloud Storage.
type Client struct {
	Buckets    []*storagex.Bucket
	mlabBucket string
	project    string
}

// Dir represents a GCS directory.
type Dir struct {
	Path string    // GCS path.
	Date time.Time // Path date.
}

// StorageReader is a Reader to a GCS object.
type StorageReader interface {
	NewReader(context.Context) (*storage.Reader, error)
}

// NewClient returns a new Client for the specified bucket names.
func NewClient(c *storage.Client, names []string, mlabBucket, project string) *Client {
	buckets := make([]*storagex.Bucket, 0)
	for _, name := range names {
		bh := c.Bucket(name)
		buckets = append(buckets, storagex.NewBucket(bh))
	}

	return &Client{
		Buckets:    buckets,
		mlabBucket: mlabBucket,
		project:    project,
	}
}

// GetDatatypes gets a list of datatypes for all the buckets
// (e.g., all datatypes under `autoload/v1/tables`).
func (c *Client) GetDatatypes(ctx context.Context) []*api.Datatype {
	prefix := path.Join(prefix, "tables")
	datatypes := make([]*api.Datatype, 0)

	for _, bucket := range c.Buckets {
		bucket.Walk(ctx, prefix, func(o *storagex.Object) error {
			file, err := ReadFile(ctx, o.ObjectHandle)
			if err != nil || len(file) == 0 {
				return fmt.Errorf("invalid schema file under %s", o.Name)
			}

			attrs, err := bucket.Attrs(ctx)
			if err != nil {
				return err
			}

			dir, filename := path.Split(o.Name)

			opts := api.DatatypeOpts{
				Name:        strings.TrimSuffix(filename, schemaFileSuffix),
				Experiment:  path.Base(dir),
				Location:    attrs.Location,
				Schema:      file,
				UpdatedTime: o.ObjectAttrs.Updated,
				Bucket:      bucket,
			}

			datatypes = append(datatypes, c.getDatatype(attrs.Name, opts))
			return nil
		})
	}

	return datatypes
}

func (c *Client) getDatatype(bucketName string, opts api.DatatypeOpts) *api.Datatype {
	switch bucketName {
	case c.mlabBucket:
		return api.NewMlabDatatype(opts)
	default:
		return api.NewThirdPartyDatatype(opts, c.project)
	}
}

// GetDirs returns all the directory paths for a datatype within a start (inclusive) and
// end (exclusive) date.
func (c *Client) GetDirs(ctx context.Context, dt *api.Datatype, start, end string) ([]Dir, error) {
	return GetDirs(ctx, dt, path.Join(prefix, dt.Experiment, dt.Name), start, end)
}

// GetDirs iterates over a set of directories and returns those whose path matches "<p>/YYYY/MM/DD"
// within a start (inclusive) and end (exclusive) date.
func GetDirs(ctx context.Context, dt *api.Datatype, p, start, end string) ([]Dir, error) {
	it := dt.Bucket.Objects(ctx, &storage.Query{
		Prefix:      p,
		StartOffset: path.Join(p, start),
		EndOffset:   path.Join(p, end),
	})

	dirMatch, err := regexp.Compile(p + datePattern)
	if err != nil {
		log.Println("failed to create regular expression:", err)
		return nil, err
	}

	dirNames := set.NewSet[string]()
	var dirs []Dir
	for {
		attr, err := it.Next()
		if err == iterator.Done {
			return dirs, nil
		}

		if err != nil {
			log.Println("failed to list bucket:", err)
			return nil, err
		}

		// Extract directory pattern match from object name.
		dirPath := dirMatch.FindString(attr.Name)
		if dirPath == "" {
			continue
		}

		// Check if directory has already been added.
		if dirNames.Contains(dirPath) {
			continue
		}
		dirNames.Add(dirPath)

		// Extract date from directory (YYYY/MM/DD).
		date := strings.TrimPrefix(dirPath, p+"/")
		format, _ := time.Parse(timex.YYYYMMDDWithSlash, date)
		dir := Dir{
			Path: "gs://" + path.Join(attr.Bucket, dirPath, "/*"),
			Date: format,
		}
		dirs = append(dirs, dir)
	}
}

// ReadFile reads a StorageReader object and returns its contents as an array of bytes.
func ReadFile(ctx context.Context, obj StorageReader) ([]byte, error) {
	reader, err := obj.NewReader(ctx)
	if err != nil {
		return nil, err
	}
	defer reader.Close()
	return io.ReadAll(reader)
}
