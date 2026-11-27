package bq

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"cloud.google.com/go/bigquery"
	"github.com/googleapis/google-cloud-go-testing/bigquery/bqiface"
	"github.com/m-lab/autoloader/api"
	apiv3 "github.com/m-lab/autoloader/api/v3"
	"github.com/m-lab/go/cloudtest/bqfake"
	"github.com/m-lab/go/testingx"
)

const testHivePrefix = "gs://archive-mlab-foo/autoload/v3"

var (
	orgSchema      = []byte(`[{"name": "id", "type": "INTEGER"}, {"name": "org", "type": "STRING"}]`)
	upperOrgSchema = []byte(`[{"name": "Org", "type": "STRING"}, {"name": "id", "type": "INTEGER"}]`)
	nestedSchema   = []byte(`[{"name": "id", "type": "INTEGER"}, {"name": "a", "type": "RECORD", "fields": [{"name": "org", "type": "STRING"}]}]`)
	brokenJSON     = []byte(`[{"name": "id",`)
)

// v3Datatype returns a v3 datatype for ndt7 with the input schema.
func v3Datatype(schema []byte) *apiv3.Datatype {
	return apiv3.NewDatatype(api.DatatypeOpts{
		Name:       "ndt7",
		Experiment: "ndt",
		Version:    "v3",
		Schema:     schema,
	}, testHivePrefix)
}

func mustSchema(b []byte) bigquery.Schema {
	s, err := bigquery.SchemaFromJSON(b)
	if err != nil {
		panic(err)
	}
	return s
}

// recordingLoader is a bqiface.Loader that keeps the configuration it is
// given, which bqfake.Loader discards.
type recordingLoader struct {
	bqiface.Loader
	cfg *bqiface.LoadConfig
	job bqiface.Job
	err error
}

func (l *recordingLoader) SetLoadConfig(cfg bqiface.LoadConfig) { l.cfg = &cfg }

func (l *recordingLoader) Run(context.Context) (bqiface.Job, error) { return l.job, l.err }

func Test_schemaV3(t *testing.T) {
	file := testingx.MustReadFile(t, "./testdata/schema.json")
	orgField := &bigquery.FieldSchema{Name: "org", Type: bigquery.StringFieldType}

	tests := []struct {
		name       string
		schema     []byte
		want       bigquery.Schema
		wantErr    bool
		wantOrgErr bool
	}{
		{
			name:   "org-appended-last",
			schema: file,
			want:   append(mustSchema(file), orgField),
		},
		{
			name:   "nested-org-allowed",
			schema: nestedSchema,
			want:   append(mustSchema(nestedSchema), orgField),
		},
		{
			name:       "org-column",
			schema:     orgSchema,
			wantErr:    true,
			wantOrgErr: true,
		},
		{
			name:       "upper-case-org-column",
			schema:     upperOrgSchema,
			wantErr:    true,
			wantOrgErr: true,
		},
		{
			name:    "invalid-json",
			schema:  brokenJSON,
			wantErr: true,
		},
		{
			name:    "invalid-schema",
			schema:  testingx.MustReadFile(t, "./testdata/invalid-schema.json"),
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := schemaV3(v3Datatype(tt.schema))
			if (err != nil) != tt.wantErr {
				t.Fatalf("schemaV3() error = %v, wantErr = %v", err, tt.wantErr)
			}
			if errors.Is(err, ErrOrgColumn) != tt.wantOrgErr {
				t.Errorf("schemaV3() error = %v, want ErrOrgColumn = %v", err, tt.wantOrgErr)
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("schemaV3() = %v, want = %v", got, tt.want)
			}
		})
	}
}

func Test_tableMetadataV3(t *testing.T) {
	file := testingx.MustReadFile(t, "./testdata/schema.json")

	t.Run("success", func(t *testing.T) {
		got, err := tableMetadataV3(v3Datatype(file))
		testingx.Must(t, err, "tableMetadataV3() failed")
		want := &bigquery.TableMetadata{
			Name:   "ndt7_raw",
			Schema: append(mustSchema(file), &bigquery.FieldSchema{Name: "org", Type: bigquery.StringFieldType}),
			TimePartitioning: &bigquery.TimePartitioning{
				Type:                   bigquery.DayPartitioningType,
				Field:                  "date",
				RequirePartitionFilter: true,
			},
			Clustering: &bigquery.Clustering{Fields: []string{"org"}},
		}
		if !reflect.DeepEqual(got, want) {
			t.Errorf("tableMetadataV3() = %+v, want = %+v", got, want)
		}
	})

	t.Run("org-column", func(t *testing.T) {
		got, err := tableMetadataV3(v3Datatype(orgSchema))
		if !errors.Is(err, ErrOrgColumn) || got != nil {
			t.Errorf("tableMetadataV3() = %v, %v, want nil, ErrOrgColumn", got, err)
		}
	})
}

func Test_hiveLoadConfig(t *testing.T) {
	schema := mustSchema(testingx.MustReadFile(t, "./testdata/schema.json"))
	uris := []string{
		testHivePrefix + "/org=alpha/ndt/ndt7/2026/11/18/*",
		testHivePrefix + "/org=foo-bar/ndt/ndt7/2026/11/18/*",
	}

	got := hiveLoadConfig(testHivePrefix, schema, uris...)

	// Every field that is set, and nothing else.
	want := bigquery.LoadConfig{
		Src: &bigquery.GCSReference{
			URIs:       uris,
			FileConfig: bigquery.FileConfig{SourceFormat: bigquery.JSON, Schema: schema},
		},
		CreateDisposition: bigquery.CreateNever,
		WriteDisposition:  bigquery.WriteTruncate,
		HivePartitioningOptions: &bigquery.HivePartitioningOptions{
			Mode:            bigquery.StringHivePartitioningMode,
			SourceURIPrefix: testHivePrefix,
		},
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("hiveLoadConfig() = %+v, want = %+v", got, want)
	}
}

func TestClient_CreateTableV3(t *testing.T) {
	tests := []struct {
		name    string
		schema  []byte
		wantErr error
	}{
		{
			name:   "success",
			schema: testingx.MustReadFile(t, "./testdata/schema.json"),
		},
		{
			name:    "org-column",
			schema:  orgSchema,
			wantErr: ErrOrgColumn,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dt := v3Datatype(tt.schema)
			md := &bigquery.TableMetadata{}
			table := bqfake.NewTable(bqfake.TableOpts{Name: dt.Table(), Metadata: md})
			ds := bqfake.NewDataset(map[string]*bqfake.Table{dt.Table(): table}, nil, nil)
			c := &Client{}

			got, err := c.CreateTableV3(context.Background(), ds, dt)
			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("Client.CreateTableV3() error = %v, want = %v", err, tt.wantErr)
			}
			if tt.wantErr != nil {
				if md.Type != "" {
					t.Errorf("Client.CreateTableV3() created a table despite error")
				}
				return
			}
			want, err := tableMetadataV3(dt)
			testingx.Must(t, err, "tableMetadataV3() failed")
			want.Type = "TABLE" // Set by bqfake on creation.
			if !reflect.DeepEqual(got, want) {
				t.Errorf("Client.CreateTableV3() = %+v, want = %+v", got, want)
			}
		})
	}
}

func TestClient_CreateTableV3_Exists(t *testing.T) {
	dt := v3Datatype(testingx.MustReadFile(t, "./testdata/schema.json"))
	table := bqfake.NewTable(bqfake.TableOpts{Name: dt.Table(), Metadata: &bigquery.TableMetadata{Type: "TABLE"}})
	ds := bqfake.NewDataset(map[string]*bqfake.Table{dt.Table(): table}, nil, nil)

	if _, err := (&Client{}).CreateTableV3(context.Background(), ds, dt); err == nil {
		t.Error("Client.CreateTableV3() error = nil, wantErr = true")
	}
}

func TestClient_UpdateSchemaV3(t *testing.T) {
	file := testingx.MustReadFile(t, "./testdata/schema.json")
	tests := []struct {
		name      string
		schema    []byte
		updateErr error
		wantErr   bool
		want      bigquery.Schema
	}{
		{
			name:   "success",
			schema: file,
			want:   append(mustSchema(file), &bigquery.FieldSchema{Name: "org", Type: bigquery.StringFieldType}),
		},
		{
			name:    "org-column",
			schema:  orgSchema,
			wantErr: true,
		},
		{
			name:      "update-err",
			schema:    file,
			updateErr: errors.New("update err"),
			wantErr:   true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dt := v3Datatype(tt.schema)
			md := &bigquery.TableMetadata{Type: "TABLE"}
			table := bqfake.NewTable(bqfake.TableOpts{Name: dt.Table(), Metadata: md, UpdateErr: tt.updateErr})
			ds := bqfake.NewDataset(map[string]*bqfake.Table{dt.Table(): table}, nil, nil)

			err := (&Client{}).UpdateSchemaV3(context.Background(), ds, dt)
			if (err != nil) != tt.wantErr {
				t.Fatalf("Client.UpdateSchemaV3() error = %v, wantErr = %v", err, tt.wantErr)
			}
			if !reflect.DeepEqual(md.Schema, tt.want) {
				t.Errorf("Client.UpdateSchemaV3() schema = %v, want = %v", md.Schema, tt.want)
			}
		})
	}
}

func TestClient_LoadHive(t *testing.T) {
	file := testingx.MustReadFile(t, "./testdata/schema.json")
	name := "ndt7_raw$20261118"
	uris := []string{
		testHivePrefix + "/org=alpha/ndt/ndt7/2026/11/18/*",
		testHivePrefix + "/org=foo-bar/ndt/ndt7/2026/11/18/*",
	}

	tests := []struct {
		name    string
		schema  []byte
		loader  *recordingLoader
		wantRun bool
		wantErr bool
	}{
		{
			name:    "success",
			schema:  file,
			loader:  &recordingLoader{job: bqfake.NewJob(&bigquery.JobStatus{}, nil)},
			wantRun: true,
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
			wantRun: true,
			wantErr: true,
		},
		{
			name:    "job-err",
			schema:  file,
			loader:  &recordingLoader{job: bqfake.NewJob(&bigquery.JobStatus{}, errors.New("job err"))},
			wantRun: true,
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dt := v3Datatype(tt.schema)
			table := bqfake.NewTable(bqfake.TableOpts{Name: name, Loader: tt.loader})
			ds := bqfake.NewDataset(map[string]*bqfake.Table{name: table}, nil, nil)

			err := (&Client{}).LoadHive(context.Background(), ds, name, dt, uris...)
			if (err != nil) != tt.wantErr {
				t.Fatalf("Client.LoadHive() error = %v, wantErr = %v", err, tt.wantErr)
			}
			if !tt.wantRun {
				if tt.loader.cfg != nil {
					t.Errorf("Client.LoadHive() configured a load despite the error")
				}
				return
			}

			// The load carries the schema file's schema, without the org column.
			want := hiveLoadConfig(testHivePrefix, mustSchema(file), uris...)
			if !reflect.DeepEqual(tt.loader.cfg.LoadConfig, want) {
				t.Errorf("Client.LoadHive() config = %+v, want = %+v", tt.loader.cfg.LoadConfig, want)
			}
			if tt.loader.cfg.Dst.TableID() != name {
				t.Errorf("Client.LoadHive() destination = %s, want = %s", tt.loader.cfg.Dst.TableID(), name)
			}
		})
	}
}
