package bq

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"cloud.google.com/go/bigquery"
	"github.com/m-lab/autoloader/api"
	"github.com/m-lab/go/cloudtest/bqfake"
	"github.com/m-lab/go/testingx"
)

var (
	orgSchema      = []byte(`[{"name": "id", "type": "INTEGER"}, {"name": "org", "type": "STRING"}]`)
	upperOrgSchema = []byte(`[{"name": "Org", "type": "STRING"}, {"name": "id", "type": "INTEGER"}]`)
	nestedSchema   = []byte(`[{"name": "id", "type": "INTEGER"}, {"name": "a", "type": "RECORD", "fields": [{"name": "org", "type": "STRING"}]}]`)
	brokenJSON     = []byte(`[{"name": "id",`)
)

func Test_tableSchema(t *testing.T) {
	file := testingx.MustReadFile(t, "./testdata/schema.json")
	orgField := &bigquery.FieldSchema{Name: "org", Type: bigquery.StringFieldType}

	tests := []struct {
		name       string
		schema     []byte
		hivePrefix string
		want       bigquery.Schema
		wantErr    bool
		wantOrgErr bool
	}{
		{
			name:   "no-prefix",
			schema: file,
			want:   mustSchema(file),
		},
		{
			name:       "prefix",
			schema:     file,
			hivePrefix: testHivePrefix,
			want:       append(mustSchema(file), orgField),
		},
		{
			name:       "prefix-nested-org-allowed",
			schema:     nestedSchema,
			hivePrefix: testHivePrefix,
			want:       append(mustSchema(nestedSchema), orgField),
		},
		{
			name:   "no-prefix-org-unchanged",
			schema: orgSchema,
			want:   mustSchema(orgSchema),
		},
		{
			name:       "prefix-org",
			schema:     orgSchema,
			hivePrefix: testHivePrefix,
			wantErr:    true,
			wantOrgErr: true,
		},
		{
			name:       "prefix-upper-org",
			schema:     upperOrgSchema,
			hivePrefix: testHivePrefix,
			wantErr:    true,
			wantOrgErr: true,
		},
		{
			name:    "no-prefix-invalid-json",
			schema:  brokenJSON,
			wantErr: true,
		},
		{
			name:       "prefix-invalid-json",
			schema:     brokenJSON,
			hivePrefix: testHivePrefix,
			wantErr:    true,
		},
		{
			name:       "prefix-invalid-schema",
			schema:     testingx.MustReadFile(t, "./testdata/invalid-schema.json"),
			hivePrefix: testHivePrefix,
			wantErr:    true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dt := &api.Datatype{
				DatatypeOpts: api.DatatypeOpts{Schema: tt.schema},
				HivePrefix:   tt.hivePrefix,
			}
			got, err := tableSchema(dt)
			if (err != nil) != tt.wantErr {
				t.Fatalf("tableSchema() error = %v, wantErr = %v", err, tt.wantErr)
			}
			if errors.Is(err, ErrOrgColumn) != tt.wantOrgErr {
				t.Fatalf("tableSchema() error = %v, want ErrOrgColumn = %v", err, tt.wantOrgErr)
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("tableSchema() = %v, want = %v", got, tt.want)
			}
		})
	}
}

func Test_tableMetadata(t *testing.T) {
	file := testingx.MustReadFile(t, "./testdata/schema.json")
	wantPartitioning := &bigquery.TimePartitioning{
		Type:                   bigquery.DayPartitioningType,
		Field:                  "date",
		RequirePartitionFilter: true,
	}

	t.Run("no-prefix", func(t *testing.T) {
		dt := api.NewMlabDatatype(api.DatatypeOpts{Name: "ndt7", Experiment: "ndt", Schema: file})
		got, err := tableMetadata(dt, dt.Table())
		testingx.Must(t, err, "tableMetadata() failed")
		// The metadata CreateTable built before shared tables existed.
		want := &bigquery.TableMetadata{
			Name:             dt.Table(),
			Schema:           mustSchema(file),
			TimePartitioning: wantPartitioning,
		}
		if !reflect.DeepEqual(got, want) {
			t.Errorf("tableMetadata() = %+v, want = %+v", got, want)
		}
	})

	t.Run("prefix", func(t *testing.T) {
		dt := hiveDatatype(file)
		got, err := tableMetadata(dt, "ndt7_foo")
		testingx.Must(t, err, "tableMetadata() failed")
		schema, err := tableSchema(dt)
		testingx.Must(t, err, "tableSchema() failed")
		want := &bigquery.TableMetadata{
			Name:             "ndt7_foo",
			Schema:           schema,
			TimePartitioning: wantPartitioning,
			Clustering:       &bigquery.Clustering{Fields: []string{"org"}},
		}
		if !reflect.DeepEqual(got, want) {
			t.Errorf("tableMetadata() = %+v, want = %+v", got, want)
		}
	})

	t.Run("org-column", func(t *testing.T) {
		got, err := tableMetadata(hiveDatatype(orgSchema), "ndt7_raw")
		if !errors.Is(err, ErrOrgColumn) || got != nil {
			t.Errorf("tableMetadata() = %v, %v, want nil, ErrOrgColumn", got, err)
		}
	})
}

func TestClient_CreateTableHive(t *testing.T) {
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
			dt := hiveDatatype(tt.schema)
			md := &bigquery.TableMetadata{}
			table := bqfake.NewTable(bqfake.TableOpts{Name: dt.Table(), Metadata: md})
			ds := bqfake.NewDataset(map[string]*bqfake.Table{dt.Table(): table}, nil, nil)
			c := &Client{}

			got, err := c.CreateTable(context.Background(), ds, dt)
			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("Client.CreateTable() error = %v, want = %v", err, tt.wantErr)
			}
			if tt.wantErr != nil {
				if md.Type != "" {
					t.Errorf("Client.CreateTable() created a table despite error")
				}
				return
			}
			want, err := tableMetadata(dt, dt.Table())
			testingx.Must(t, err, "tableMetadata() failed")
			want.Type = "TABLE" // Set by bqfake on creation.
			if !reflect.DeepEqual(got, want) {
				t.Errorf("Client.CreateTable() = %+v, want = %+v", got, want)
			}
		})
	}
}

func TestClient_UpdateSchemaHive(t *testing.T) {
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
			dt := hiveDatatype(tt.schema)
			md := &bigquery.TableMetadata{Type: "TABLE"}
			table := bqfake.NewTable(bqfake.TableOpts{Name: dt.Table(), Metadata: md})
			ds := bqfake.NewDataset(map[string]*bqfake.Table{dt.Table(): table}, nil, nil)
			c := &Client{}

			err := c.UpdateSchema(context.Background(), ds, dt)
			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("Client.UpdateSchema() error = %v, want = %v", err, tt.wantErr)
			}
			if tt.wantErr != nil {
				if md.Schema != nil {
					t.Errorf("Client.UpdateSchema() updated the schema despite error")
				}
				return
			}
			want, err := tableSchema(dt)
			testingx.Must(t, err, "tableSchema() failed")
			if !reflect.DeepEqual(md.Schema, want) {
				t.Errorf("Client.UpdateSchema() schema = %v, want = %v", md.Schema, want)
			}
		})
	}
}
