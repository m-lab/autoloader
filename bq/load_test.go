package bq

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"cloud.google.com/go/bigquery"
	"github.com/m-lab/go/cloudtest/bqfake"
	"github.com/m-lab/go/testingx"
)

func TestClient_Load(t *testing.T) {
	tests := []struct {
		name    string
		loader  *bqfake.Loader
		uris    []string
		wantErr bool
	}{
		{
			name:    "success",
			loader:  bqfake.NewLoader(bqfake.NewJob(&bigquery.JobStatus{}, nil), nil),
			wantErr: false,
		},
		{
			name:   "success-multiple-uris",
			loader: bqfake.NewLoader(bqfake.NewJob(&bigquery.JobStatus{}, nil), nil),
			uris: []string{
				"gs://fake-bucket/autoload/v1/experiment/datatype/2023/03/26/*",
				"gs://fake-bucket/autoload/v1/experiment/datatype/2023/03/27/*",
				"gs://fake-bucket/autoload/v1/experiment/datatype/2023/03/28/*",
			},
			wantErr: false,
		},
		{
			name: "loader-err",
			loader: bqfake.NewLoader(bqfake.NewJob(&bigquery.JobStatus{}, nil),
				errors.New("loader err")),
			wantErr: true,
		},
		{
			name: "job-err",
			loader: bqfake.NewLoader(bqfake.NewJob(&bigquery.JobStatus{}, errors.New("job error")),
				nil),
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			opts := bqfake.TableOpts{
				Dataset:  bqfake.Dataset{},
				Name:     datatypeID,
				Metadata: &bigquery.TableMetadata{},
				Loader:   tt.loader,
			}
			table := bqfake.NewTable(opts)
			ds := bqfake.NewDataset(map[string]*bqfake.Table{datatypeID: table}, nil, nil)
			bq, err := bqfake.NewClient(context.Background(), projectID, map[string]*bqfake.Dataset{experimentID: ds})
			testingx.Must(t, err, "failed to create fake bq client")
			c := &Client{Client: bq}

			uris := append(tt.uris, "gs://fake-bucket/autoload/v1/experiment/datatype/YYYY/MM/DD/*")
			err = c.Load(context.Background(), ds, datatypeID, uris...)
			if (err != nil) != tt.wantErr {
				t.Fatalf("Client.Load() error = %v, wantErr = %v", err, tt.wantErr)
			}
		})
	}
}

func TestClient_jobErrors(t *testing.T) {
	err1 := &bigquery.Error{
		Message: "Error1",
	}
	err2 := &bigquery.Error{
		Message: "Error2",
	}
	tests := []struct {
		name    string
		status  *bigquery.JobStatus
		wantErr bool
		wantNum int
	}{
		{
			name:    "no-error",
			status:  &bigquery.JobStatus{},
			wantErr: false,
		},
		{
			name: "one-error",
			status: &bigquery.JobStatus{
				Errors: []*bigquery.Error{err1},
			},
			wantErr: true,
			wantNum: 1,
		},
		{
			name: "multiple-errors",
			status: &bigquery.JobStatus{
				Errors: []*bigquery.Error{err1, err2},
			},
			wantErr: true,
			wantNum: 2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := jobErrors(tt.status)
			if (err != nil) != tt.wantErr {
				t.Fatalf("jobErrors() error = %v, wantErr = %v", err, tt.wantErr)
			}

			if !tt.wantErr {
				return
			}

			u, ok := err.(interface {
				Unwrap() []error
			})
			if !ok {
				t.Fatal("jobErrors() failed to cast returned error")
			}

			gotNum := len(u.Unwrap())
			if gotNum != tt.wantNum {
				t.Fatalf("jobErrors() gotNum = %d, wantNum = %d", gotNum, tt.wantNum)
			}
		})
	}
}

func Test_newLoadConfig(t *testing.T) {
	schema := mustSchema(testingx.MustReadFile(t, "./testdata/schema.json"))
	uris := []string{
		testHivePrefix + "/org=alpha/ndt/ndt7/2026/11/18/*",
		testHivePrefix + "/org=foo-bar/ndt/ndt7/2026/11/18/*",
	}
	tests := []struct {
		name       string
		hivePrefix string
		schema     bigquery.Schema
		wantSchema bigquery.Schema
		wantHive   *bigquery.HivePartitioningOptions
	}{
		{
			name: "no-prefix",
		},
		{
			name:   "no-prefix-schema-ignored",
			schema: schema,
		},
		{
			name:       "prefix",
			hivePrefix: testHivePrefix,
			schema:     schema,
			wantSchema: schema,
			wantHive: &bigquery.HivePartitioningOptions{
				Mode:            bigquery.StringHivePartitioningMode,
				SourceURIPrefix: testHivePrefix,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := newLoadConfig(tt.hivePrefix, tt.schema, uris...)

			src, ok := got.Src.(*bigquery.GCSReference)
			if !ok {
				t.Fatalf("newLoadConfig().Src = %T, want *bigquery.GCSReference", got.Src)
			}
			if !reflect.DeepEqual(src.URIs, uris) {
				t.Errorf("newLoadConfig().Src.URIs = %v, want = %v", src.URIs, uris)
			}
			if src.SourceFormat != bigquery.JSON {
				t.Errorf("newLoadConfig().Src.SourceFormat = %v, want = %v", src.SourceFormat, bigquery.JSON)
			}
			if !reflect.DeepEqual(src.Schema, tt.wantSchema) {
				t.Errorf("newLoadConfig().Src.Schema = %v, want = %v", src.Schema, tt.wantSchema)
			}
			if got.WriteDisposition != bigquery.WriteTruncate {
				t.Errorf("newLoadConfig().WriteDisposition = %v, want = %v", got.WriteDisposition, bigquery.WriteTruncate)
			}
			if !reflect.DeepEqual(got.HivePartitioningOptions, tt.wantHive) {
				t.Errorf("newLoadConfig().HivePartitioningOptions = %+v, want = %+v", got.HivePartitioningOptions, tt.wantHive)
			}

			// Nothing else is set.
			want := bigquery.LoadConfig{
				Src: &bigquery.GCSReference{
					URIs:       uris,
					FileConfig: bigquery.FileConfig{SourceFormat: bigquery.JSON, Schema: tt.wantSchema},
				},
				WriteDisposition:        bigquery.WriteTruncate,
				HivePartitioningOptions: tt.wantHive,
			}
			if !reflect.DeepEqual(got, want) {
				t.Errorf("newLoadConfig() = %+v, want = %+v", got, want)
			}
		})
	}
}

func TestClient_LoadConfig(t *testing.T) {
	loader := &recordingLoader{job: bqfake.NewJob(&bigquery.JobStatus{}, nil)}
	table := bqfake.NewTable(bqfake.TableOpts{Name: datatypeID, Metadata: &bigquery.TableMetadata{}, Loader: loader})
	ds := bqfake.NewDataset(map[string]*bqfake.Table{datatypeID: table}, nil, nil)
	c := &Client{}

	uri := "gs://fake-bucket/autoload/v2/experiment/datatype/2023/03/26/*"
	err := c.Load(context.Background(), ds, datatypeID, uri)
	testingx.Must(t, err, "Client.Load() failed")

	if loader.cfg == nil {
		t.Fatal("Client.Load() did not set a load config")
	}
	if !reflect.DeepEqual(loader.cfg.LoadConfig, newLoadConfig("", nil, uri)) {
		t.Errorf("Client.Load() config = %+v, want = %+v", loader.cfg.LoadConfig, newLoadConfig("", nil, uri))
	}
	if loader.cfg.Dst != table {
		t.Errorf("Client.Load() Dst = %v, want = %v", loader.cfg.Dst, table)
	}
}

func TestClient_LoadHive(t *testing.T) {
	file := testingx.MustReadFile(t, "./testdata/schema.json")
	uris := []string{
		testHivePrefix + "/org=alpha/ndt/ndt7/2026/11/18/*",
		testHivePrefix + "/org=foo-bar/ndt/ndt7/2026/11/18/*",
	}
	name := "ndt7_raw$20261118"
	tests := []struct {
		name    string
		schema  []byte
		loader  *recordingLoader
		wantErr bool
	}{
		{
			name:   "success",
			schema: file,
			loader: &recordingLoader{job: bqfake.NewJob(&bigquery.JobStatus{}, nil)},
		},
		{
			name:    "invalid-schema",
			schema:  brokenJSON,
			loader:  &recordingLoader{job: bqfake.NewJob(&bigquery.JobStatus{}, nil)},
			wantErr: true,
		},
		{
			name:    "loader-err",
			schema:  file,
			loader:  &recordingLoader{err: errors.New("loader err")},
			wantErr: true,
		},
		{
			name:    "job-err",
			schema:  file,
			loader:  &recordingLoader{job: bqfake.NewJob(&bigquery.JobStatus{}, errors.New("job error"))},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dt := hiveDatatype(tt.schema)
			table := bqfake.NewTable(bqfake.TableOpts{Name: name, Metadata: &bigquery.TableMetadata{}, Loader: tt.loader})
			ds := bqfake.NewDataset(map[string]*bqfake.Table{name: table}, nil, nil)
			c := &Client{}

			err := c.LoadHive(context.Background(), ds, name, dt, uris...)
			if (err != nil) != tt.wantErr {
				t.Fatalf("Client.LoadHive() error = %v, wantErr = %v", err, tt.wantErr)
			}
			if tt.name == "invalid-schema" {
				if tt.loader.cfg != nil {
					t.Errorf("Client.LoadHive() configured a load despite an invalid schema")
				}
				return
			}

			// The load carries the schema file's schema, without the org column.
			want := newLoadConfig(testHivePrefix, mustSchema(file), uris...)
			if tt.loader.cfg == nil {
				t.Fatal("Client.LoadHive() did not set a load config")
			}
			if !reflect.DeepEqual(tt.loader.cfg.LoadConfig, want) {
				t.Errorf("Client.LoadHive() config = %+v, want = %+v", tt.loader.cfg.LoadConfig, want)
			}
			if tt.loader.cfg.Dst != table {
				t.Errorf("Client.LoadHive() Dst = %v, want = %v", tt.loader.cfg.Dst, table)
			}
		})
	}
}
