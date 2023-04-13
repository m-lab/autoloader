package gcs

import (
	"bytes"
	"context"
	"errors"
	"path"
	"testing"
	"time"

	"cloud.google.com/go/storage"
	"github.com/fsouza/fake-gcs-server/fakestorage"
	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/m-lab/autoloader/api"
	"github.com/m-lab/go/storagex"
	"github.com/m-lab/go/testingx"
)

var (
	testBucket = "test-bucket"
)

func TestClient_GetDatatypes(t *testing.T) {
	updated := time.Date(02, 02, 2023, 3, 15, 0, 0, time.UTC)

	tests := []struct {
		name       string
		objs       []fakestorage.Object
		names      []string
		mlabBucket string
		want       []*api.Datatype
	}{
		{
			name: "success",
			objs: []fakestorage.Object{
				{
					ObjectAttrs: fakestorage.ObjectAttrs{
						BucketName: testBucket,
						Name:       path.Join(prefix, "tables/experiment1/datatype1"),
						Updated:    updated,
					},
					Content: testingx.MustReadFile(t, "testdata/experiment1/datatype1.table.json"),
				},
			},
			names: []string{testBucket},
			want: []*api.Datatype{
				{
					Name:        "datatype1",
					Experiment:  "experiment1",
					Location:    "US",
					Schema:      testingx.MustReadFile(t, "testdata/experiment1/datatype1.table.json"),
					UpdatedTime: updated,
					Bucket: &storagex.Bucket{
						BucketHandle: &storage.BucketHandle{},
					},
				},
			},
		},
		{
			name: "success-with-mlab-bucket",
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
						BucketName: "archive-non-mlab",
						Name:       path.Join(prefix, "tables/experiment2/datatype2"),
						Updated:    updated,
					},
					Content: testingx.MustReadFile(t, "testdata/experiment2/datatype2.table.json"),
				},
			},
			names:      []string{"archive-mlab-sandbox", "archive-non-mlab"},
			mlabBucket: "archive-mlab-sandbox",
			want: []*api.Datatype{
				{
					Name:        "datatype1",
					Experiment:  "raw_experiment1",
					Location:    "US",
					Schema:      testingx.MustReadFile(t, "testdata/experiment1/datatype1.table.json"),
					UpdatedTime: updated,
					Bucket: &storagex.Bucket{
						BucketHandle: &storage.BucketHandle{},
					},
				},
				{
					Name:        "datatype2",
					Experiment:  "experiment2",
					Location:    "US",
					Schema:      testingx.MustReadFile(t, "testdata/experiment2/datatype2.table.json"),
					UpdatedTime: updated,
					Bucket: &storagex.Bucket{
						BucketHandle: &storage.BucketHandle{},
					},
				},
			},
		},
		{
			name: "invalid-schema-file",
			objs: []fakestorage.Object{
				{
					ObjectAttrs: fakestorage.ObjectAttrs{
						BucketName: testBucket,
						Name:       path.Join(prefix, "tables/experiment1/datatype1"),
					},
					Content: nil,
				},
			},
			names: []string{testBucket},
			want:  []*api.Datatype{},
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
			c := NewClient(server.Client(), tt.names, tt.mlabBucket)

			got := c.GetDatatypes(context.TODO())
			if !cmp.Equal(got, tt.want, cmpopts.IgnoreUnexported(storagex.Bucket{}, storage.BucketHandle{})) {
				t.Errorf("Client.GetDatatypes() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestGetDirs(t *testing.T) {
	tests := []struct {
		name    string
		objs    []fakestorage.Object
		start   string
		end     string
		dt      string
		exp     string
		want    []Dir
		wantErr bool
	}{
		{
			name: "success",
			objs: []fakestorage.Object{
				{
					ObjectAttrs: fakestorage.ObjectAttrs{
						BucketName: testBucket,
						Name:       prefix + "experiment1/datatype1/2023/03/06/filename.jsonl.gz",
					},
				},
			},
			dt:    "datatype1",
			exp:   "experiment1",
			start: "2023/03/05",
			end:   "2023/03/07",
			want: []Dir{
				{
					Path: "gs://" + path.Join(testBucket, prefix, "experiment1/datatype1/2023/03/06/*"),
					Date: time.Date(2023, 03, 06, 0, 0, 0, 0, time.UTC),
				},
			},
		},
		{
			name: "success-with-mlab-bucket",
			objs: []fakestorage.Object{
				{
					ObjectAttrs: fakestorage.ObjectAttrs{
						BucketName: testBucket,
						Name:       prefix + "experiment1/datatype1/2023/03/06/filename.jsonl.gz",
					},
				},
			},
			dt:    "datatype1",
			exp:   "raw_experiment1",
			start: "2023/03/05",
			end:   "2023/03/07",
			want: []Dir{
				{
					Path: "gs://" + path.Join(testBucket, prefix, "experiment1/datatype1/2023/03/06/*"),
					Date: time.Date(2023, 03, 06, 0, 0, 0, 0, time.UTC),
				},
			},
		},
		{
			name: "success-multiple-objs-same-dir",
			objs: []fakestorage.Object{
				{
					ObjectAttrs: fakestorage.ObjectAttrs{
						BucketName: testBucket,
						Name:       prefix + "experiment1/datatype1/2023/03/06/",
					},
				},
				{
					ObjectAttrs: fakestorage.ObjectAttrs{
						BucketName: testBucket,
						Name:       prefix + "experiment1/datatype1/2023/03/06/filename.jsonl.gz",
					},
				},
				{
					ObjectAttrs: fakestorage.ObjectAttrs{
						BucketName: testBucket,
						Name:       prefix + "experiment1/datatype1/2023/03/06/filename2.jsonl.gz",
					},
				},
			},
			dt:    "datatype1",
			exp:   "experiment1",
			start: "2023/03/05",
			end:   "2023/03/07",
			want: []Dir{
				{
					Path: "gs://" + path.Join(testBucket, prefix, "experiment1/datatype1/2023/03/06/*"),
					Date: time.Date(2023, 03, 06, 0, 0, 0, 0, time.UTC),
				},
			},
		},
		{
			name: "incorrect-dir-path",
			objs: []fakestorage.Object{
				{
					ObjectAttrs: fakestorage.ObjectAttrs{
						BucketName: testBucket,
						Name:       prefix + "incorrect-experiment/datatype1/2023/03/06/filename.jsonl.gz",
					},
				},
			},
			dt:    "datatype1",
			exp:   "experiment1",
			start: "2023/03/05",
			end:   "2023/03/07",
			want:  []Dir{},
		},
		{
			name: "incorrect-time-format",
			objs: []fakestorage.Object{
				{
					ObjectAttrs: fakestorage.ObjectAttrs{
						BucketName: testBucket,
						Name:       prefix + "experiment1/datatype1/03/06/2023/filename.jsonl.gz",
					},
				},
			},
			dt:    "datatype1",
			exp:   "experiment1",
			start: "2023/03/05",
			end:   "2023/03/07",
			want:  []Dir{},
		},
		{
			name: "incorrect-date-range",
			objs: []fakestorage.Object{
				{
					ObjectAttrs: fakestorage.ObjectAttrs{
						BucketName: testBucket,
						Name:       prefix + "experiment1/datatype1/2023/03/06/filename.jsonl.gz",
					},
				},
			},
			dt:    "datatype1",
			exp:   "experiment1",
			start: "2023/03/05",
			end:   "2023/03/06",
			want:  []Dir{},
		},
		{
			name:    "inexistent-bucket",
			objs:    []fakestorage.Object{},
			dt:      "datatype1",
			exp:     "experiment1",
			start:   "2023/03/05",
			end:     "2023/03/07",
			wantErr: true,
			want:    []Dir{},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			server, err := fakestorage.NewServerWithOptions(fakestorage.Options{
				InitialObjects: tt.objs,
			})
			testingx.Must(t, err, "error initializing GCS server")
			defer server.Stop()
			client := server.Client()

			dt := &api.Datatype{
				Name:       tt.dt,
				Experiment: tt.exp,
				Bucket: &storagex.Bucket{
					BucketHandle: client.Bucket(testBucket),
				},
			}

			c := &Client{}
			got, err := c.GetDirs(context.TODO(), dt, tt.start, tt.end)
			if (err != nil) != tt.wantErr {
				t.Errorf("Client.GetDirs() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			if !cmp.Equal(got, tt.want, cmpopts.EquateEmpty()) {
				t.Errorf("Client.GetDirs() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestReadFileSuccess(t *testing.T) {
	want := []byte("bar")
	server, err := fakestorage.NewServerWithOptions(fakestorage.Options{
		InitialObjects: []fakestorage.Object{
			{
				ObjectAttrs: fakestorage.ObjectAttrs{
					BucketName: testBucket,
					Name:       "foo",
				},
				Content: want,
			},
		},
	})
	testingx.Must(t, err, "error initializing GCS server")
	defer server.Stop()
	client := server.Client()
	obj := client.Bucket(testBucket).Object("foo")

	got, err := readFile(context.TODO(), obj)
	if err != nil {
		t.Errorf("readFile() error = %v, wantErr = false", err)
	}
	if !bytes.Equal(got, want) {
		t.Errorf("readFile() = %v, want = = %v", got, want)
	}
}

func TestReadFileError(t *testing.T) {
	got, err := readFile(context.TODO(), &fakeErrReader{})
	if err == nil {
		t.Errorf("readFile() error = nil, wantErr = true")
	}
	if got != nil {
		t.Errorf("readFile() = %v, want = nil", got)
	}
}

type fakeErrReader struct{}

func (r *fakeErrReader) NewReader(context.Context) (*storage.Reader, error) {
	return nil, errors.New("error")
}
