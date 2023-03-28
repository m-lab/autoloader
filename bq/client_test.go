package bq

import (
	"context"
	"errors"
	"testing"

	"cloud.google.com/go/bigquery"
	"github.com/googleapis/google-cloud-go-testing/bigquery/bqiface"
	"github.com/m-lab/autoloader/api"
	"github.com/m-lab/go/cloudtest/bqfake"
	"github.com/m-lab/go/testingx"
)

var (
	projectID = "project"
	datasetID = "dataset"
	tableID   = "table"
)

func TestClient_GetDataset(t *testing.T) {
	ds := bqfake.NewDataset(nil, &bqiface.DatasetMetadata{
		DatasetMetadata: bigquery.DatasetMetadata{
			Name: datasetID,
		},
	}, nil)

	tests := []struct {
		name     string
		datasets map[string]*bqfake.Dataset
		want     *bqfake.Dataset
		wantErr  bool
	}{
		{
			name:     "exists",
			datasets: map[string]*bqfake.Dataset{datasetID: ds},
			want:     ds,
			wantErr:  false,
		},
		{
			name:     "not-exists",
			datasets: map[string]*bqfake.Dataset{},
			wantErr:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			bq, err := bqfake.NewClient(context.TODO(), projectID, tt.datasets)
			testingx.Must(t, err, "failed to create fake bq client")
			c := &Client{bq}

			got, err := c.GetDataset(context.Background(), datasetID)
			if (err != nil) != tt.wantErr {
				t.Fatalf("Client.GetDataset() error = %v, wantErr = %v", err, tt.wantErr)
			}

			if tt.wantErr {
				return
			}

			if got != tt.want {
				t.Errorf("Client.GetDataset() = %v, want = %v", got, tt.want)
			}
		})
	}
}

func TestClient_CreateDataset(t *testing.T) {
	tests := []struct {
		name    string
		dataset *bqfake.Dataset
		wantErr bool
	}{
		{
			name:    "success",
			dataset: bqfake.NewDataset(nil, nil, nil),
			wantErr: false,
		},
		{
			name:    "error",
			dataset: bqfake.NewDataset(nil, nil, errors.New("create dataset error")),
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			bq, err := bqfake.NewClient(context.TODO(), projectID, map[string]*bqfake.Dataset{datasetID: tt.dataset})
			testingx.Must(t, err, "failed to create fake bq client")
			c := &Client{bq}

			got, err := c.CreateDataset(context.Background(), &api.Datatype{Experiment: datasetID})
			if (err != nil) != tt.wantErr {
				t.Fatalf("Client.CreateDataset() error = %v, wantErr = %v", err, tt.wantErr)
			}

			if tt.wantErr {
				return
			}

			if got != tt.dataset {
				t.Errorf("Client.CreateDataset() = %v, want = %v", got, tt.dataset)
			}
		})
	}
}

func TestClient_GetTableMetadata(t *testing.T) {
	tests := []struct {
		name    string
		md      *bigquery.TableMetadata
		wantErr bool
	}{
		{
			name:    "success",
			md:      &bigquery.TableMetadata{Type: "type"},
			wantErr: false,
		},
		{
			name:    "no-metadata",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			opts := bqfake.TableOpts{
				Dataset:  bqfake.Dataset{},
				Name:     tableID,
				Metadata: tt.md,
			}
			table := bqfake.NewTable(opts)
			ds := bqfake.NewDataset(map[string]*bqfake.Table{tableID: table}, nil, nil)
			bq, err := bqfake.NewClient(context.TODO(), projectID, map[string]*bqfake.Dataset{datasetID: ds})
			testingx.Must(t, err, "failed to create fake bq client")
			c := &Client{bq}

			got, err := c.GetTableMetadata(context.Background(), ds, tableID)
			if (err != nil) != tt.wantErr {
				t.Fatalf("Client.GetTableMetadata() error = %v, wantErr = %v", err, tt.wantErr)
			}

			if tt.wantErr {
				return
			}

			if got != tt.md {
				t.Errorf("Client.GetTableMetadata() = %v, want = %v", got, tt.md)
			}
		})
	}
}

func TestClient_CreateTable(t *testing.T) {
	tests := []struct {
		name    string
		schema  []byte
		wantErr bool
	}{
		{
			name:    "success",
			schema:  testingx.MustReadFile(t, "./testdata/schema.json"),
			wantErr: false,
		},
		{
			name:    "invalid-schema",
			schema:  testingx.MustReadFile(t, "./testdata/invalid-schema.json"),
			wantErr: true,
		},
		{
			name:    "no-schema",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			opts := bqfake.TableOpts{
				Dataset:  bqfake.Dataset{},
				Name:     tableID,
				Metadata: &bigquery.TableMetadata{},
			}
			table := bqfake.NewTable(opts)
			ds := bqfake.NewDataset(map[string]*bqfake.Table{tableID: table}, nil, nil)
			bq, err := bqfake.NewClient(context.TODO(), projectID, map[string]*bqfake.Dataset{datasetID: ds})
			testingx.Must(t, err, "failed to create fake bq client")
			c := &Client{bq}

			dt := &api.Datatype{Name: tableID, Schema: tt.schema}

			err = c.CreateTable(context.Background(), ds, dt)
			if (err != nil) != tt.wantErr {
				t.Fatalf("Client.CreateTable() error = %v, wantErr = %v", err, tt.wantErr)
			}
		})
	}
}

func TestClient_UpdateSchema(t *testing.T) {
	tests := []struct {
		name    string
		schema  []byte
		wantErr bool
	}{
		{
			name:    "success",
			schema:  testingx.MustReadFile(t, "./testdata/schema.json"),
			wantErr: false,
		},
		{
			name:    "invalid-schema",
			schema:  testingx.MustReadFile(t, "./testdata/invalid-schema.json"),
			wantErr: true,
		},
		{
			name:    "no-schema",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			opts := bqfake.TableOpts{
				Dataset:  bqfake.Dataset{},
				Name:     tableID,
				Metadata: &bigquery.TableMetadata{},
			}
			table := bqfake.NewTable(opts)
			ds := bqfake.NewDataset(map[string]*bqfake.Table{tableID: table}, nil, nil)
			bq, err := bqfake.NewClient(context.TODO(), projectID, map[string]*bqfake.Dataset{datasetID: ds})
			testingx.Must(t, err, "failed to create fake bq client")
			c := &Client{bq}

			dt := &api.Datatype{Name: tableID, Schema: tt.schema}

			err = c.UpdateSchema(context.Background(), ds, dt)
			if (err != nil) != tt.wantErr {
				t.Fatalf("Client.UpdateSchema() error = %v, wantErr = %v", err, tt.wantErr)
			}
		})
	}
}

func TestClient_Load(t *testing.T) {
	tests := []struct {
		name    string
		loader  *bqfake.Loader
		wantErr bool
	}{
		{
			name:    "success",
			loader:  bqfake.NewLoader(*bqfake.NewJob(&bigquery.JobStatus{}, nil), nil),
			wantErr: false,
		},
		{
			name: "loader-err",
			loader: bqfake.NewLoader(*bqfake.NewJob(&bigquery.JobStatus{}, nil),
				errors.New("loader err")),
			wantErr: true,
		},
		{
			name: "job-err",
			loader: bqfake.NewLoader(*bqfake.NewJob(&bigquery.JobStatus{}, errors.New("job error")),
				nil),
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			opts := bqfake.TableOpts{
				Dataset:  bqfake.Dataset{},
				Name:     tableID,
				Metadata: &bigquery.TableMetadata{},
				Loader:   tt.loader,
			}
			table := bqfake.NewTable(opts)
			ds := bqfake.NewDataset(map[string]*bqfake.Table{tableID: table}, nil, nil)
			bq, err := bqfake.NewClient(context.TODO(), projectID, map[string]*bqfake.Dataset{datasetID: ds})
			testingx.Must(t, err, "failed to create fake bq client")
			c := &Client{bq}

			err = c.Load(context.Background(), ds, tableID, "gs://fake-bucket/autoload/v1/experiment/datatype/YYYY/MM/DD/*")
			if (err != nil) != tt.wantErr {
				t.Fatalf("Client.Load() error = %v, wantErr = %v", err, tt.wantErr)
			}
		})
	}
}
