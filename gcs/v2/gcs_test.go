package v2

import (
	"context"
	"path"
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
			name: "success-mlab",
			objs: []fakestorage.Object{
				{
					ObjectAttrs: fakestorage.ObjectAttrs{
						BucketName: "archive-mlab-sandbox",
						Name:       path.Join(prefix, "tables/experiment1/datatype1"),
						Updated:    updated,
					},
					Content: testingx.MustReadFile(t, "testdata/experiment1/datatype1.table.json"),
				},
				{
					ObjectAttrs: fakestorage.ObjectAttrs{
						BucketName: "archive-mlab-sandbox",
						Name:       path.Join(prefix, "mlab/experiment1/datatype1/2023/03/06/filename.jsonl.gz"),
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
			},
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
			if !cmp.Equal(got, tt.want, cmpopts.IgnoreUnexported(storagex.Bucket{}, storage.BucketHandle{})) {
				t.Errorf("Client.GetDatatypes() = %v, want %v", got, tt.want)
			}
		})
	}
}
