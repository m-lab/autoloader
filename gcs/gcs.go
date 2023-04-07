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
	"github.com/m-lab/autoloader/api"
	"github.com/m-lab/go/storagex"
	"github.com/m-lab/go/timex"
	"google.golang.org/api/iterator"
)

var (
	suffix = regexp.MustCompile(`(\d{4}/[01]\d/[0123]\d)/$`)
)

const (
	prefix           = "autoload/v1/"
	schemaFileSuffix = ".table.json"
)

// Client is used to interact with Google Cloud Storage.
type Client struct {
	Buckets    []*storagex.Bucket
	mlabBucket string
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
func NewClient(c *storage.Client, names []string, mlabBucket string) *Client {
	buckets := make([]*storagex.Bucket, 0)
	for _, name := range names {
		bh := c.Bucket(name)
		buckets = append(buckets, storagex.NewBucket(bh))
	}

	return &Client{
		Buckets:    buckets,
		mlabBucket: mlabBucket,
	}
}

// GetDatatypes gets a list of datatypes for all the buckets
// (e.g., all datatypes under `autoload/v1/tables`).
func (c *Client) GetDatatypes(ctx context.Context) []*api.Datatype {
	prefix := path.Join(prefix, "tables")
	datatypes := make([]*api.Datatype, 0)

	for _, bucket := range c.Buckets {
		bucket.Walk(ctx, prefix, func(o *storagex.Object) error {
			file, err := readFile(ctx, o.ObjectHandle)
			if err != nil || len(file) == 0 {
				return fmt.Errorf("invalid schema file under %s", o.Name)
			}

			attrs, err := bucket.Attrs(ctx)
			if err != nil {
				return err
			}

			dir, filename := path.Split(o.Name)
			exp := path.Base(dir)
			ds := exp
			if attrs.Name == c.mlabBucket {
				ds = "raw_" + ds
			}

			s := &api.Datatype{
				Name:        strings.TrimSuffix(filename, schemaFileSuffix),
				Experiment:  exp,
				Dataset:     ds,
				Location:    attrs.Location,
				Schema:      file,
				UpdatedTime: o.ObjectAttrs.Updated,
				Bucket:      bucket,
			}
			datatypes = append(datatypes, s)
			return nil
		})
	}

	return datatypes
}

// GetDirs returns all the directory paths for a datatype within a start (inclusive) and
// end (exclusive) date.
func (c *Client) GetDirs(ctx context.Context, dt *api.Datatype, start, end string) ([]Dir, error) {
	prefix := path.Join(prefix, dt.Experiment, dt.Name)
	it := dt.Bucket.Objects(ctx, &storage.Query{
		Prefix:      prefix,
		StartOffset: path.Join(prefix, start),
		EndOffset:   path.Join(prefix, end),
	})

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

		date := suffix.FindString(attr.Name)
		if date == "" {
			continue
		}

		format, err := time.Parse(timex.YYYYMMDDWithSlash+"/", date)
		if err != nil {
			continue
		}

		dir := Dir{
			Path: "gs://" + path.Join(attr.Bucket, attr.Name, "*"),
			Date: format,
		}
		dirs = append(dirs, dir)
	}
}

func readFile(ctx context.Context, obj StorageReader) ([]byte, error) {
	reader, err := obj.NewReader(ctx)
	if err != nil {
		return nil, err
	}
	defer reader.Close()
	return io.ReadAll(reader)
}
