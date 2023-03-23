package bq

import (
	"context"
	"errors"
	"testing"

	"cloud.google.com/go/bigquery"
	"github.com/goccy/bigquery-emulator/server"
	"github.com/googleapis/google-cloud-go-testing/bigquery/bqiface"
	"github.com/m-lab/autoloader/api"
	"github.com/m-lab/go/cloudtest/bqfake"
	"github.com/m-lab/go/testingx"
	"google.golang.org/api/option"
)

var (
	projectID = "project"
	datasetID = "dataset"
	tableID   = "table"
)

func mustSetUpTest(t *testing.T, src server.Source) (*bigquery.Client, func()) {
	bqServer, err := server.New(server.TempStorage)
	testingx.Must(t, err, "could not create BQ test server")

	err = bqServer.Load(src)
	testingx.Must(t, err, "could not load BQ test source")

	err = bqServer.SetProject(projectID)
	testingx.Must(t, err, "could not set BQ test project")

	testServer := bqServer.TestServer()

	ctx := context.Background()
	client, err := bigquery.NewClient(ctx, projectID, option.WithEndpoint(testServer.URL), option.WithoutAuthentication())
	testingx.Must(t, err, "could not start BQ test client")

	return client, func() {
		testServer.Close()
		client.Close()
	}
}

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
			table := bqfake.NewTable(bqfake.Dataset{}, tableID, tt.md, nil)
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
		schema  string
		wantErr bool
	}{
		{
			name:    "success",
			schema:  "./testdata/schema.json",
			wantErr: false,
		},
		{
			name:    "invalid-schema",
			schema:  "./testdata/invalid-schema.json",
			wantErr: true,
		},
		{
			name:    "no-schema",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			table := bqfake.NewTable(bqfake.Dataset{}, tableID, &bigquery.TableMetadata{}, nil)
			ds := bqfake.NewDataset(map[string]*bqfake.Table{tableID: table}, nil, nil)
			bq, err := bqfake.NewClient(context.TODO(), projectID, map[string]*bqfake.Dataset{datasetID: ds})
			testingx.Must(t, err, "failed to create fake bq client")
			c := &Client{bq}

			dt := &api.Datatype{Name: tableID}
			if tt.schema != "" {
				dt.Schema = testingx.MustReadFile(t, tt.schema)
			}

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
		schema  string
		wantErr bool
	}{
		{
			name:    "success",
			schema:  "./testdata/schema.json",
			wantErr: false,
		},
		{
			name:    "invalid-schema",
			schema:  "./testdata/invalid-schema.json",
			wantErr: true,
		},
		{
			name:    "no-schema",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			table := bqfake.NewTable(bqfake.Dataset{}, tableID, &bigquery.TableMetadata{}, nil)
			ds := bqfake.NewDataset(map[string]*bqfake.Table{tableID: table}, nil, nil)
			bq, err := bqfake.NewClient(context.TODO(), projectID, map[string]*bqfake.Dataset{datasetID: ds})
			testingx.Must(t, err, "failed to create fake bq client")
			c := &Client{bq}

			dt := &api.Datatype{
				Name: tableID,
			}
			if tt.schema != "" {
				dt.Schema = testingx.MustReadFile(t, tt.schema)
			}

			err = c.UpdateSchema(context.Background(), ds, dt)
			if (err != nil) != tt.wantErr {
				t.Fatalf("Client.UpdateSchema() error = %v, wantErr = %v", err, tt.wantErr)
			}
		})
	}
}
