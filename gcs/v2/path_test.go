package v2

import (
	"context"
	"path"
	"reflect"
	"testing"

	"github.com/fsouza/fake-gcs-server/fakestorage"
	"github.com/m-lab/go/storagex"
	"github.com/m-lab/go/testingx"
)

var testBucket = "test-bucket"

func TestNewPath(t *testing.T) {
	tests := []struct {
		name       string
		schemaPath string
		objs       []fakestorage.Object
		orgs       []string
		want       *SchemaPath
		wantErr    bool
	}{
		{
			name:       "in-band",
			schemaPath: path.Join(prefix, "tables/organization1/experiment1/datatype1.table.json"),
			want: &SchemaPath{
				Experiment:    "experiment1",
				Datatype:      "datatype1",
				Organizations: []string{"organization1"},
			},
			wantErr: false,
		},
		{
			name:       "out-of-band",
			schemaPath: path.Join(prefix, "tables/experiment1/datatype1.table.json"),
			objs: []fakestorage.Object{
				{
					ObjectAttrs: fakestorage.ObjectAttrs{
						BucketName: testBucket,
						Name:       path.Join(prefix, "organization1/experiment1/datatype1/2023/03/06/filename.jsonl.gz"),
					},
				},
				{
					ObjectAttrs: fakestorage.ObjectAttrs{
						BucketName: testBucket,
						Name:       path.Join(prefix, "organization2/experiment1/datatype1/2023/03/06/filename.jsonl.gz"),
					},
				},
				{
					ObjectAttrs: fakestorage.ObjectAttrs{
						BucketName: testBucket,
						Name:       path.Join(prefix, "organization3/experiment1/different-datatype/2023/03/06/filename.jsonl.gz"),
					},
				},
			},
			orgs: []string{"organization1", "organization2", "organization3"},
			want: &SchemaPath{
				Experiment:    "experiment1",
				Datatype:      "datatype1",
				Organizations: []string{"organization1", "organization2"},
			},
			wantErr: false,
		},
		{
			name:       "out-of-band-no-data",
			schemaPath: path.Join(prefix, "tables/experiment1/datatype1.table.json"),
			objs:       []fakestorage.Object{},
			orgs:       []string{"organization1", "organization2", "organization3"},
			want: &SchemaPath{
				Experiment:    "experiment1",
				Datatype:      "datatype1",
				Organizations: []string{},
			},
			wantErr: false,
		},
		{
			name:       "invalid-path",
			schemaPath: path.Join(prefix),
			want:       nil,
			wantErr:    true,
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
			bucket := &BucketV2{
				Bucket: &storagex.Bucket{
					BucketHandle: client.Bucket(testBucket),
				},
				Organizations: tt.orgs,
			}

			got, err := NewSchemaPath(context.Background(), bucket, tt.schemaPath)
			if (err != nil) != tt.wantErr {
				t.Errorf("NewPath() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("NewPath() = %v, want %v", got, tt.want)
			}
		})
	}
}
