package v2

import (
	"context"
	"path"
	"sort"
	"testing"
	"time"

	"cloud.google.com/go/storage"
	"github.com/fsouza/fake-gcs-server/fakestorage"
	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/m-lab/autoloader/api"
	apiv2 "github.com/m-lab/autoloader/api/v2"
	"github.com/m-lab/go/storagex"
	"github.com/m-lab/go/testingx"
)

func TestClient_GetDatatypes(t *testing.T) {
	updated := time.Date(02, 02, 2023, 3, 15, 0, 0, time.UTC)

	tests := []struct {
		name  string
		names []string
		objs  []fakestorage.Object
		want  []*api.Datatype
	}{
		{
			name: "mlab",
			objs: []fakestorage.Object{
				// Out-of-band schema.
				{
					ObjectAttrs: fakestorage.ObjectAttrs{
						BucketName: "archive-mlab-sandbox",
						Name:       path.Join(prefix, "tables/experiment1/datatype1"),
						Updated:    updated,
					},
					Content: testingx.MustReadFile(t, "testdata/experiment1/datatype1.table.json"),
				},
				// In-band schema.
				{
					ObjectAttrs: fakestorage.ObjectAttrs{
						BucketName: "archive-mlab-sandbox",
						Name:       path.Join(prefix, "tables/mlab/experiment2/datatype2"),
						Updated:    updated,
					},
					Content: testingx.MustReadFile(t, "testdata/mlab/experiment2/datatype2.table.json"),
				},
				// Data.
				{
					ObjectAttrs: fakestorage.ObjectAttrs{
						BucketName: "archive-mlab-sandbox",
						Name:       path.Join(prefix, "mlab/experiment1/datatype1/2023/03/06/filename.jsonl.gz"),
					},
				},
				{
					ObjectAttrs: fakestorage.ObjectAttrs{
						BucketName: "archive-mlab-sandbox",
						Name:       path.Join(prefix, "mlab/experiment2/datatype2/2023/03/06/filename.jsonl.gz"),
					},
				},
			},
			names: []string{"archive-mlab-sandbox"},
			want: []*api.Datatype{
				apiv2.NewMlabDatatype(
					api.DatatypeOpts{
						Name:         "datatype1",
						Experiment:   "experiment1",
						Organization: "mlab",
						Version:      "v2",
						Location:     "US",
						Schema:       testingx.MustReadFile(t, "testdata/experiment1/datatype1.table.json"),
						UpdatedTime:  updated,
						Bucket: &storagex.Bucket{
							BucketHandle: &storage.BucketHandle{},
						},
					}),
				apiv2.NewMlabDatatype(
					api.DatatypeOpts{
						Name:         "datatype2",
						Experiment:   "experiment2",
						Organization: "mlab",
						Version:      "v2",
						Location:     "US",
						Schema:       testingx.MustReadFile(t, "testdata/mlab/experiment2/datatype2.table.json"),
						UpdatedTime:  updated,
						Bucket: &storagex.Bucket{
							BucketHandle: &storage.BucketHandle{},
						},
					}),
			},
		},
		{
			name: "autojoin",
			objs: []fakestorage.Object{
				// Out-of-band schema.
				{
					ObjectAttrs: fakestorage.ObjectAttrs{
						BucketName: "archive-mlab-autojoin",
						Name:       path.Join(prefix, "tables/experiment1/datatype1"),
						Updated:    updated,
					},
					Content: testingx.MustReadFile(t, "testdata/experiment1/datatype1.table.json"),
				},
				// In-band schema.
				{
					ObjectAttrs: fakestorage.ObjectAttrs{
						BucketName: "archive-mlab-autojoin",
						Name:       path.Join(prefix, "tables/autojoin-org/experiment3/datatype3"),
						Updated:    updated,
					},
					Content: testingx.MustReadFile(t, "testdata/autojoin-org/experiment3/datatype3.table.json"),
				},
				// Data.
				{
					ObjectAttrs: fakestorage.ObjectAttrs{
						BucketName: "archive-mlab-autojoin",
						Name:       path.Join(prefix, "autojoin-org/experiment1/datatype1/2023/03/06/filename.jsonl.gz"),
					},
				},
				{
					ObjectAttrs: fakestorage.ObjectAttrs{
						BucketName: "archive-mlab-autojoin",
						Name:       path.Join(prefix, "autojoin-org/experiment3/datatype3/2023/03/06/filename.jsonl.gz"),
					},
				},
			},
			names: []string{"archive-mlab-autojoin"},
			want: []*api.Datatype{
				apiv2.NewBYODatatype(
					api.DatatypeOpts{
						Name:         "datatype1",
						Experiment:   "experiment1",
						Organization: "autojoin-org",
						Version:      "v2",
						Location:     "US",
						Schema:       testingx.MustReadFile(t, "testdata/experiment1/datatype1.table.json"),
						UpdatedTime:  updated,
						Bucket: &storagex.Bucket{
							BucketHandle: &storage.BucketHandle{},
						},
					}, "mlab-autojoin",
				),
				apiv2.NewBYODatatype(
					api.DatatypeOpts{
						Name:         "datatype3",
						Experiment:   "experiment3",
						Organization: "autojoin-org",
						Version:      "v2",
						Location:     "US",
						Schema:       testingx.MustReadFile(t, "testdata/autojoin-org/experiment3/datatype3.table.json"),
						UpdatedTime:  updated,
						Bucket: &storagex.Bucket{
							BucketHandle: &storage.BucketHandle{},
						},
					}, "mlab-autojoin",
				),
			},
		},
		{
			name: "thirdparty",
			objs: []fakestorage.Object{
				// Out-of-band schema.
				{
					ObjectAttrs: fakestorage.ObjectAttrs{
						BucketName: "archive-mlab-thirdparty",
						Name:       path.Join(prefix, "tables/experiment1/datatype1"),
						Updated:    updated,
					},
					Content: testingx.MustReadFile(t, "testdata/experiment1/datatype1.table.json"),
				},
				// In-band schema.
				{
					ObjectAttrs: fakestorage.ObjectAttrs{
						BucketName: "archive-mlab-thirdparty",
						Name:       path.Join(prefix, "tables/thirdparty-org/experiment4/datatype4"),
						Updated:    updated,
					},
					Content: testingx.MustReadFile(t, "testdata/thirdparty-org/experiment4/datatype4.table.json"),
				},
				// Data.
				{
					ObjectAttrs: fakestorage.ObjectAttrs{
						BucketName: "archive-mlab-thirdparty",
						Name:       path.Join(prefix, "thirdparty-org/experiment1/datatype1/2023/03/06/filename.jsonl.gz"),
					},
				},
				{
					ObjectAttrs: fakestorage.ObjectAttrs{
						BucketName: "archive-mlab-thirdparty",
						Name:       path.Join(prefix, "thirdparty-org/experiment4/datatype4/2023/03/06/filename.jsonl.gz"),
					},
				},
			},
			names: []string{"archive-mlab-thirdparty"},
			want: []*api.Datatype{
				apiv2.NewBYODatatype(
					api.DatatypeOpts{
						Name:         "datatype1",
						Experiment:   "experiment1",
						Organization: "thirdparty-org",
						Version:      "v2",
						Location:     "US",
						Schema:       testingx.MustReadFile(t, "testdata/experiment1/datatype1.table.json"),
						UpdatedTime:  updated,
						Bucket: &storagex.Bucket{
							BucketHandle: &storage.BucketHandle{},
						},
					}, "mlab-thirdparty",
				),
				apiv2.NewBYODatatype(
					api.DatatypeOpts{
						Name:         "datatype4",
						Experiment:   "experiment4",
						Organization: "thirdparty-org",
						Version:      "v2",
						Location:     "US",
						Schema:       testingx.MustReadFile(t, "testdata/thirdparty-org/experiment4/datatype4.table.json"),
						UpdatedTime:  updated,
						Bucket: &storagex.Bucket{
							BucketHandle: &storage.BucketHandle{},
						},
					}, "mlab-thirdparty",
				),
			},
		},
		{
			name:  "inexistent-bucket",
			names: []string{"archive-not-existent"},
			objs:  []fakestorage.Object{},
			want:  []*api.Datatype{},
		},
		{
			name:  "invalid-schema-file",
			names: []string{"archive-mlab-sandbox"},
			objs: []fakestorage.Object{
				{
					ObjectAttrs: fakestorage.ObjectAttrs{
						BucketName: "archive-mlab-sandbox",
						Name:       path.Join(prefix, "tables/experiment1/datatype1"),
						Updated:    updated,
					},
					Content: nil, // nil file.
				},
			},
			want: []*api.Datatype{},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			server, err := fakestorage.NewServerWithOptions(fakestorage.Options{
				InitialObjects:  tt.objs,
				BucketsLocation: "US",
			})
			testingx.Must(t, err, "error initializing GCS server")
			defer server.Stop()
			c := NewClient(server.Client(), tt.names)

			got := c.GetDatatypes(context.Background())
			sortDatatypes(got)
			if !cmp.Equal(got, tt.want, cmpopts.IgnoreUnexported(storagex.Bucket{}, storage.BucketHandle{})) {
				t.Errorf("Client.GetDatatypes() = %v, want %v", got, tt.want)
			}
		})
	}
}

func sortDatatypes(dts []*api.Datatype) {
	sort.Slice(dts, func(i, j int) bool {
		return dts[i].Name < dts[j].Name
	})
}
