package v3

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
	apiv3 "github.com/m-lab/autoloader/api/v3"
	"github.com/m-lab/go/storagex"
	"github.com/m-lab/go/testingx"
)

const testBucket = "archive-mlab-foo"

var schema = []byte(`[{"name": "id", "type": "STRING", "mode": "NULLABLE"}]`)

func obj(bucket, name string, content []byte) fakestorage.Object {
	return fakestorage.Object{
		ObjectAttrs: fakestorage.ObjectAttrs{
			BucketName: bucket,
			Name:       name,
			Updated:    time.Date(2023, 02, 02, 3, 15, 0, 0, time.UTC),
		},
		Content: content,
	}
}

func seed() []fakestorage.Object {
	return []fakestorage.Object{
		obj(testBucket, prefix+"tables/ndt/ndt7.table.json", schema),
		obj(testBucket, prefix+"tables/ndt/extra/deep.table.json", schema),
		obj(testBucket, prefix+"tables-old/ndt/ndt7.table.json", schema),
		obj(testBucket, prefix+"org=alpha/ndt/ndt7/2026/11/18/a.jsonl.gz", nil),
		obj(testBucket, prefix+"org=alpha/ndt/ndt7/2026/11/19/a.jsonl.gz", nil),
		obj(testBucket, prefix+"org=alpha/ndt/index1/2026/11/18/a.jsonl.gz", nil),
		obj(testBucket, prefix+"org=foo-bar/ndt/ndt7/2026/11/18/b.jsonl.gz", nil),
		obj(testBucket, prefix+"org=empty/ndt/annotation2/2026/11/18/c.jsonl.gz", nil),
		obj(testBucket, prefix+"notanorg/ndt/ndt7/2026/11/18/d.jsonl.gz", nil),
	}
}

func TestClient_GetDatatypes(t *testing.T) {
	tests := []struct {
		name    string
		buckets []string
		objs    []fakestorage.Object
		want    []*apiv3.Datatype
	}{
		{
			name:    "seed",
			buckets: []string{testBucket},
			objs:    seed(),
			want: []*apiv3.Datatype{
				apiv3.NewDatatype(api.DatatypeOpts{
					Name:        "ndt7",
					Experiment:  "ndt",
					Version:     "v3",
					Location:    "US",
					Schema:      schema,
					UpdatedTime: time.Date(2023, 02, 02, 3, 15, 0, 0, time.UTC),
					Bucket:      &storagex.Bucket{BucketHandle: &storage.BucketHandle{}},
				}, "gs://"+testBucket+"/autoload/v3"),
			},
		},
		{
			// The same experiment and datatype in a second bucket would share
			// the destination table: only the first bucket's is kept.
			name:    "duplicate-in-second-bucket",
			buckets: []string{testBucket, "archive-mlab-bar"},
			objs: []fakestorage.Object{
				obj(testBucket, prefix+"tables/ndt/ndt7.table.json", schema),
				obj("archive-mlab-bar", prefix+"tables/ndt/ndt7.table.json", schema),
				obj("archive-mlab-bar", prefix+"tables/ndt/scamper2.table.json", schema),
			},
			want: []*apiv3.Datatype{
				apiv3.NewDatatype(api.DatatypeOpts{
					Name:        "ndt7",
					Experiment:  "ndt",
					Version:     "v3",
					Location:    "US",
					Schema:      schema,
					UpdatedTime: time.Date(2023, 02, 02, 3, 15, 0, 0, time.UTC),
					Bucket:      &storagex.Bucket{BucketHandle: &storage.BucketHandle{}},
				}, "gs://"+testBucket+"/autoload/v3"),
				apiv3.NewDatatype(api.DatatypeOpts{
					Name:        "scamper2",
					Experiment:  "ndt",
					Version:     "v3",
					Location:    "US",
					Schema:      schema,
					UpdatedTime: time.Date(2023, 02, 02, 3, 15, 0, 0, time.UTC),
					Bucket:      &storagex.Bucket{BucketHandle: &storage.BucketHandle{}},
				}, "gs://archive-mlab-bar/autoload/v3"),
			},
		},
		{
			name:    "inexistent-bucket",
			buckets: []string{"archive-not-existent"},
			objs:    []fakestorage.Object{},
			want:    []*apiv3.Datatype{},
		},
		{
			name:    "invalid-schema-file",
			buckets: []string{testBucket},
			objs: []fakestorage.Object{
				obj(testBucket, prefix+"tables/ndt/ndt7.table.json", nil),
			},
			want: []*apiv3.Datatype{},
		},
		{
			name:    "not-a-schema-file",
			buckets: []string{testBucket},
			objs: []fakestorage.Object{
				obj(testBucket, prefix+"tables/ndt/ndt7.txt", schema),
			},
			want: []*apiv3.Datatype{},
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
			c := NewClient(server.Client(), tt.buckets)

			got := c.GetDatatypes(context.Background())
			if !cmp.Equal(got, tt.want, cmpopts.IgnoreUnexported(storagex.Bucket{}, storage.BucketHandle{})) {
				t.Errorf("Client.GetDatatypes() = %v, want %v", got, tt.want)
			}
		})
	}
}

func dirPath(org, date string) string {
	return "gs://" + path.Join(testBucket, prefix, "org="+org, "ndt/ndt7", date, "*")
}

func TestClient_GetDirs(t *testing.T) {
	tests := []struct {
		name    string
		objs    []fakestorage.Object
		start   string
		end     string
		want    []Dir
		wantErr bool
	}{
		{
			name:  "all-orgs",
			objs:  seed(),
			start: "2026/11/18",
			end:   "2026/11/20",
			want: []Dir{
				{Org: "alpha", Path: dirPath("alpha", "2026/11/18"), Date: time.Date(2026, 11, 18, 0, 0, 0, 0, time.UTC)},
				{Org: "alpha", Path: dirPath("alpha", "2026/11/19"), Date: time.Date(2026, 11, 19, 0, 0, 0, 0, time.UTC)},
				{Org: "foo-bar", Path: dirPath("foo-bar", "2026/11/18"), Date: time.Date(2026, 11, 18, 0, 0, 0, 0, time.UTC)},
			},
		},
		{
			name:  "orgs-without-objects-omitted",
			objs:  seed(),
			start: "2026/11/19",
			end:   "2026/11/20",
			want: []Dir{
				{Org: "alpha", Path: dirPath("alpha", "2026/11/19"), Date: time.Date(2026, 11, 19, 0, 0, 0, 0, time.UTC)},
			},
		},
		{
			name:  "no-objects-in-range",
			objs:  seed(),
			start: "2026/12/01",
			end:   "2026/12/02",
			want:  []Dir{},
		},
		{
			name:    "inexistent-bucket",
			objs:    []fakestorage.Object{},
			start:   "2026/11/18",
			end:     "2026/11/20",
			want:    []Dir{},
			wantErr: true,
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

			dt := apiv3.NewDatatype(api.DatatypeOpts{
				Name:       "ndt7",
				Experiment: "ndt",
				Version:    "v3",
				Bucket:     &storagex.Bucket{BucketHandle: client.Bucket(testBucket)},
			}, "gs://"+testBucket+"/autoload/v3")

			c := &Client{}
			got, err := c.GetDirs(context.Background(), dt, tt.start, tt.end)
			if (err != nil) != tt.wantErr {
				t.Errorf("Client.GetDirs() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			// Newest comes from object creation times, which fakestorage assigns.
			if !cmp.Equal(got, tt.want, cmpopts.EquateEmpty(), cmpopts.IgnoreFields(Dir{}, "Newest")) {
				t.Errorf("Client.GetDirs() = %v, want %v", got, tt.want)
			}
			for _, d := range got {
				if d.Newest.IsZero() {
					t.Errorf("Client.GetDirs() dir %s has zero Newest", d.Path)
				}
			}
		})
	}
}

func TestGetBucketOrgs(t *testing.T) {
	server, err := fakestorage.NewServerWithOptions(fakestorage.Options{InitialObjects: seed()})
	testingx.Must(t, err, "error initializing GCS server")
	defer server.Stop()

	b := &storagex.Bucket{BucketHandle: server.Client().Bucket(testBucket)}
	got, err := getBucketOrgs(context.Background(), b)
	testingx.Must(t, err, "getBucketOrgs() failed")

	// "tables" and "notanorg" are not organizations.
	want := []string{"alpha", "empty", "foo-bar"}
	if !cmp.Equal(got, want) {
		t.Errorf("getBucketOrgs() = %v, want %v", got, want)
	}
}

func TestClient_GetDirs_InvalidDate(t *testing.T) {
	// A path that looks like a date but is not one is skipped, and so is a
	// date directory that is not followed by an object name.
	server, err := fakestorage.NewServerWithOptions(fakestorage.Options{
		InitialObjects: []fakestorage.Object{
			obj(testBucket, prefix+"org=alpha/ndt/ndt7/2026/11/31/a.jsonl.gz", nil),
			obj(testBucket, prefix+"org=alpha/ndt/ndt7/2026/11/300/a.jsonl.gz", nil),
			obj(testBucket, prefix+"org=alpha/ndt/ndt7/2026/11/30/a.jsonl.gz", nil),
		},
	})
	testingx.Must(t, err, "error initializing GCS server")
	defer server.Stop()

	dt := apiv3.NewDatatype(api.DatatypeOpts{
		Name:       "ndt7",
		Experiment: "ndt",
		Version:    "v3",
		Bucket:     &storagex.Bucket{BucketHandle: server.Client().Bucket(testBucket)},
	}, "gs://"+testBucket+"/autoload/v3")

	got, err := (&Client{}).GetDirs(context.Background(), dt, "2026/11/01", "2026/12/01")
	testingx.Must(t, err, "failed to get dirs")
	if len(got) != 1 || got[0].Path != dirPath("alpha", "2026/11/30") {
		t.Errorf("Client.GetDirs() = %v, want only the directory of 2026/11/30", got)
	}
}

func TestClient_GetDirs_Newest(t *testing.T) {
	// Two objects in one directory: Newest is the creation time of the newer
	// one, whether it is listed first or second.
	older := time.Date(2026, 11, 18, 5, 0, 0, 0, time.UTC)
	newer := time.Date(2026, 11, 20, 7, 0, 0, 0, time.UTC)
	dir := prefix + "org=alpha/ndt/ndt7/2026/11/18/"

	tests := []struct {
		name          string
		first, second time.Time
	}{
		{name: "newer-listed-first", first: newer, second: older},
		{name: "newer-listed-second", first: older, second: newer},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			first, second := obj(testBucket, dir+"a.jsonl.gz", nil), obj(testBucket, dir+"b.jsonl.gz", nil)
			first.Created, second.Created = tt.first, tt.second

			server, err := fakestorage.NewServerWithOptions(fakestorage.Options{
				InitialObjects: []fakestorage.Object{first, second},
			})
			testingx.Must(t, err, "error initializing GCS server")
			defer server.Stop()

			dt := apiv3.NewDatatype(api.DatatypeOpts{
				Name:       "ndt7",
				Experiment: "ndt",
				Version:    "v3",
				Bucket:     &storagex.Bucket{BucketHandle: server.Client().Bucket(testBucket)},
			}, "gs://"+testBucket+"/autoload/v3")

			got, err := (&Client{}).GetDirs(context.Background(), dt, "2026/11/18", "2026/11/20")
			testingx.Must(t, err, "failed to get dirs")
			if len(got) != 1 || got[0].Org != "alpha" || !got[0].Newest.Equal(newer) {
				t.Errorf("Client.GetDirs() = %v, want one dir of alpha with Newest %v", got, newer)
			}
		})
	}
}
