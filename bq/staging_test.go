package bq

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"cloud.google.com/go/bigquery"
	"github.com/googleapis/google-cloud-go-testing/bigquery/bqiface"
	"github.com/m-lab/go/cloudtest/bqfake"
	"github.com/m-lab/go/testingx"
)

func TestStagingDataset(t *testing.T) {
	dt := hiveDatatype(nil)
	if got := StagingDataset(dt); got != "autoload_v3_ndt_staging" {
		t.Errorf("StagingDataset() = %q, want = %q", got, "autoload_v3_ndt_staging")
	}
}

func TestStagingTable(t *testing.T) {
	tests := []struct {
		org  string
		want string
	}{
		{org: "mlab", want: "ndt7_mlab"},
		{org: "foo-bar", want: "ndt7_foo_bar"},
		{org: "a-b-c1", want: "ndt7_a_b_c1"},
	}
	dt := hiveDatatype(nil)
	for _, tt := range tests {
		t.Run(tt.org, func(t *testing.T) {
			if got := StagingTable(dt, tt.org); got != tt.want {
				t.Errorf("StagingTable() = %q, want = %q", got, tt.want)
			}
		})
	}
}

func TestClient_EnsureStagingDataset(t *testing.T) {
	dt := hiveDatatype(nil)
	exists := &bqiface.DatasetMetadata{DatasetMetadata: bigquery.DatasetMetadata{Name: StagingDataset(dt)}}
	tests := []struct {
		name        string
		dataset     *bqfake.Dataset
		wantCreated bool
		wantErr     bool
	}{
		{
			name:        "create",
			dataset:     bqfake.NewDataset(nil, nil, nil),
			wantCreated: true,
		},
		{
			// Create would fail, so it must not be called.
			name:    "exists",
			dataset: bqfake.NewDataset(nil, exists, errors.New("create should not be called")),
		},
		{
			name:        "create-error",
			dataset:     bqfake.NewDataset(nil, nil, errors.New("create dataset error")),
			wantCreated: true,
			wantErr:     true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rds := &recordingDataset{Dataset: tt.dataset}
			c := &Client{Client: recordingClient{ds: rds}}

			got, err := c.EnsureStagingDataset(context.Background(), dt)
			if (err != nil) != tt.wantErr {
				t.Fatalf("Client.EnsureStagingDataset() error = %v, wantErr = %v", err, tt.wantErr)
			}
			if (rds.created != nil) != tt.wantCreated {
				t.Fatalf("Client.EnsureStagingDataset() created = %v, want = %v", rds.created, tt.wantCreated)
			}
			if tt.wantCreated {
				want := &bqiface.DatasetMetadata{DatasetMetadata: bigquery.DatasetMetadata{
					Name:                   "autoload_v3_ndt_staging",
					Location:               "US",
					DefaultTableExpiration: 48 * time.Hour,
				}}
				if !reflect.DeepEqual(rds.created, want) {
					t.Errorf("Client.EnsureStagingDataset() created with %+v, want = %+v", rds.created, want)
				}
			}
			if tt.wantErr {
				if got != nil {
					t.Errorf("Client.EnsureStagingDataset() = %v, want nil on error", got)
				}
				return
			}
			if got != rds {
				t.Errorf("Client.EnsureStagingDataset() = %v, want = %v", got, rds)
			}
		})
	}
}

func TestClient_EnsureStagingDataset_bqfake(t *testing.T) {
	// The same paths through the plain bqfake client.
	dt := hiveDatatype(nil)
	exists := bqfake.NewDataset(nil, &bqiface.DatasetMetadata{}, nil)
	for name, datasets := range map[string]map[string]*bqfake.Dataset{
		"create": {},
		"exists": {StagingDataset(dt): exists},
	} {
		t.Run(name, func(t *testing.T) {
			bq, err := bqfake.NewClient(context.Background(), projectID, datasets)
			testingx.Must(t, err, "failed to create fake bq client")
			c := &Client{Client: bq}

			got, err := c.EnsureStagingDataset(context.Background(), dt)
			testingx.Must(t, err, "Client.EnsureStagingDataset() failed")
			if got != datasets[StagingDataset(dt)] {
				t.Errorf("Client.EnsureStagingDataset() = %v, want = %v", got, datasets[StagingDataset(dt)])
			}
		})
	}
}

func TestClient_EnsureStagingTable(t *testing.T) {
	file := testingx.MustReadFile(t, "./testdata/schema.json")
	name := "ndt7_foo_bar"
	oldSchema := bigquery.Schema{{Name: "id", Type: bigquery.IntegerFieldType}}
	tests := []struct {
		name       string
		schema     []byte
		md         *bigquery.TableMetadata
		updateErr  error
		wantCreate bool
		wantErr    error
		anyErr     bool
	}{
		{
			name:       "create",
			schema:     file,
			md:         &bigquery.TableMetadata{},
			wantCreate: true,
		},
		{
			name:   "exists-update-schema",
			schema: file,
			md:     &bigquery.TableMetadata{Type: "TABLE", Schema: oldSchema},
		},
		{
			name:      "update-error",
			schema:    file,
			md:        &bigquery.TableMetadata{Type: "TABLE", Schema: oldSchema},
			updateErr: errors.New("update error"),
			anyErr:    true,
		},
		{
			// bqfake fails both Metadata and Create on a table without metadata.
			name:   "create-error",
			schema: file,
			anyErr: true,
		},
		{
			name:    "org-column",
			schema:  orgSchema,
			md:      &bigquery.TableMetadata{},
			wantErr: ErrOrgColumn,
			anyErr:  true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dt := hiveDatatype(tt.schema)
			// bqfake.Table.Metadata needs a dataset with a project ID to
			// report a missing table, which only bqfake.Client provides.
			bq, err := bqfake.NewClient(context.Background(), projectID, map[string]*bqfake.Dataset{})
			testingx.Must(t, err, "failed to create fake bq client")
			parent := bq.Dataset(StagingDataset(dt)).(*bqfake.Dataset)
			table := bqfake.NewTable(bqfake.TableOpts{Dataset: *parent, Name: name, Metadata: tt.md, UpdateErr: tt.updateErr})
			ds := bqfake.NewDataset(map[string]*bqfake.Table{name: table}, nil, nil)
			c := &Client{}

			err = c.EnsureStagingTable(context.Background(), ds, dt, "foo-bar")
			if (err != nil) != tt.anyErr {
				t.Fatalf("Client.EnsureStagingTable() error = %v, wantErr = %v", err, tt.anyErr)
			}
			if tt.wantErr != nil && !errors.Is(err, tt.wantErr) {
				t.Fatalf("Client.EnsureStagingTable() error = %v, want = %v", err, tt.wantErr)
			}
			if tt.anyErr {
				return
			}

			want, err := tableMetadata(dt, name)
			testingx.Must(t, err, "tableMetadata() failed")
			if tt.wantCreate {
				// Same schema, partitioning and clustering as the shared table.
				want.Type = "TABLE" // Set by bqfake on creation.
				if !reflect.DeepEqual(tt.md, want) {
					t.Errorf("Client.EnsureStagingTable() created %+v, want = %+v", tt.md, want)
				}
				return
			}
			if !reflect.DeepEqual(tt.md.Schema, want.Schema) {
				t.Errorf("Client.EnsureStagingTable() schema = %v, want = %v", tt.md.Schema, want.Schema)
			}
		})
	}
}

func TestClient_CopyPartitions(t *testing.T) {
	newTable := func(name string, copier *recordingCopier) bqiface.Table {
		return copyTable{
			Table:  bqfake.NewTable(bqfake.TableOpts{Name: name, Metadata: &bigquery.TableMetadata{}}),
			copier: copier,
		}
	}
	tests := []struct {
		name     string
		copier   *recordingCopier
		srcs     int
		wantRuns int
		wantErr  bool
	}{
		{
			name:    "no-sources",
			copier:  &recordingCopier{job: bqfake.NewJob(&bigquery.JobStatus{}, nil)},
			wantErr: true,
		},
		{
			name:     "one-source",
			copier:   &recordingCopier{job: bqfake.NewJob(&bigquery.JobStatus{}, nil)},
			srcs:     1,
			wantRuns: 1,
		},
		{
			name:     "two-sources",
			copier:   &recordingCopier{job: bqfake.NewJob(&bigquery.JobStatus{}, nil)},
			srcs:     2,
			wantRuns: 1,
		},
		{
			name:     "run-error",
			copier:   &recordingCopier{err: errors.New("run error")},
			srcs:     2,
			wantRuns: 1,
			wantErr:  true,
		},
		{
			name:     "job-error",
			copier:   &recordingCopier{job: bqfake.NewJob(&bigquery.JobStatus{}, errors.New("job error"))},
			srcs:     2,
			wantRuns: 1,
			wantErr:  true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dst := newTable("ndt7_raw$20261118", tt.copier)
			names := []string{"ndt7_alpha$20261118", "ndt7_foo_bar$20261118"}
			var srcs []bqiface.Table
			for _, n := range names[:tt.srcs] {
				srcs = append(srcs, newTable(n, nil))
			}
			c := &Client{}

			err := c.CopyPartitions(context.Background(), dst, srcs...)
			if (err != nil) != tt.wantErr {
				t.Fatalf("Client.CopyPartitions() error = %v, wantErr = %v", err, tt.wantErr)
			}
			if tt.copier.runs != tt.wantRuns {
				t.Fatalf("Client.CopyPartitions() ran %d jobs, want = %d", tt.copier.runs, tt.wantRuns)
			}
			if tt.srcs == 0 {
				if !errors.Is(err, errNoSources) || tt.copier.cfg != nil || tt.copier.from != nil {
					t.Errorf("Client.CopyPartitions() with no sources = %v, configured = %v", err, tt.copier.cfg != nil)
				}
				return
			}

			if !reflect.DeepEqual(tt.copier.from, srcs) {
				t.Errorf("Client.CopyPartitions() CopierFrom(%v), want = %v", tt.copier.from, srcs)
			}
			cfg := tt.copier.cfg
			if cfg == nil {
				t.Fatal("Client.CopyPartitions() did not set a copy config")
			}
			if !reflect.DeepEqual(cfg.Srcs, srcs) || cfg.Dst != dst {
				t.Errorf("Client.CopyPartitions() config Srcs = %v, Dst = %v, want = %v, %v", cfg.Srcs, cfg.Dst, srcs, dst)
			}
			want := bigquery.CopyConfig{
				CreateDisposition: bigquery.CreateNever,
				WriteDisposition:  bigquery.WriteTruncate,
			}
			if !reflect.DeepEqual(cfg.CopyConfig, want) {
				t.Errorf("Client.CopyPartitions() config = %+v, want = %+v", cfg.CopyConfig, want)
			}
		})
	}
}
