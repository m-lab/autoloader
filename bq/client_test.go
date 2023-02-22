package bq

import (
	"context"
	"os"
	"testing"

	"cloud.google.com/go/bigquery"
	"github.com/goccy/bigquery-emulator/server"
	"github.com/goccy/bigquery-emulator/types"
	"github.com/m-lab/autoloader/api"
	"github.com/m-lab/go/testingx"
	"google.golang.org/api/option"
)

var (
	projectID = "project"
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
	datasetID := "dataset-get"
	ds := types.NewDataset(datasetID)

	client, teardown := mustSetUpTest(t, server.StructSource(types.NewProject(projectID, ds)))
	defer teardown()
	c := &Client{
		Client: client,
	}

	tests := []struct {
		name    string
		dsID    string
		wantErr bool
		want    *bigquery.Dataset
	}{
		{
			name:    "exists",
			dsID:    datasetID,
			wantErr: false,
			want: &bigquery.Dataset{
				ProjectID: projectID,
				DatasetID: datasetID,
			},
		},
		{
			name:    "not-exists",
			dsID:    "nonexistent-dataset",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := c.GetDataset(context.Background(), tt.dsID)
			if (err != nil) != tt.wantErr {
				t.Errorf("Client.GetDataset() error = %v, wantErr = %v", err, tt.wantErr)
				return
			}

			if !tt.wantErr && (got.ProjectID != tt.want.ProjectID || got.DatasetID != tt.want.DatasetID) {
				t.Errorf("Client.GetDataset() = %v, want = %v", got, tt.want)
			}
		})
	}
}

func TestClient_CreateDataset(t *testing.T) {
	datasetID := "dataset-create"
	dt := &api.Datatype{
		Experiment: datasetID,
	}
	want := &bigquery.Dataset{
		ProjectID: projectID,
		DatasetID: datasetID,
	}

	client, teardown := mustSetUpTest(t, server.StructSource(types.NewProject(projectID)))
	defer teardown()
	c := &Client{
		Client: client,
	}

	got, err := c.CreateDataset(context.Background(), dt)
	if err != nil {
		t.Errorf("Client.CreateDataset() error = %v, wantErr = nil", err)
		return
	}

	if got.ProjectID != want.ProjectID || got.DatasetID != want.DatasetID {
		t.Errorf("Client.CreateDataset() = %v, want = %v",
			got, want)
	}
}

func TestClient_GetTableMetadata(t *testing.T) {
	tableID := "table-get-metadata"
	datasetID := "dataset-table-get-metadata"
	table := types.NewTable(
		tableID,
		[]*types.Column{
			types.NewColumn("id", types.INTEGER),
			types.NewColumn("name", types.STRING),
		},
		types.Data{},
	)
	ds := types.NewDataset(datasetID, table)

	client, teardown := mustSetUpTest(t, server.StructSource(types.NewProject(projectID, ds)))
	defer teardown()
	c := &Client{
		Client: client,
	}

	tests := []struct {
		name    string
		tID     string
		wantErr bool
		want    *bigquery.TableMetadata
	}{
		{
			name:    "exists",
			tID:     tableID,
			wantErr: false,
			want: &bigquery.TableMetadata{
				FullID: projectID + ":" + datasetID + "." + tableID,
				Schema: bigquery.Schema{
					&bigquery.FieldSchema{
						Name: "id",
						Type: bigquery.IntegerFieldType,
					},
					&bigquery.FieldSchema{
						Name: "name",
						Type: bigquery.StringFieldType,
					},
				},
			},
		},
		{
			name:    "not-exists",
			tID:     "nonexistent-table",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := c.GetTableMetadata(context.Background(), c.Dataset(datasetID), tt.tID)
			if (err != nil) != tt.wantErr {
				t.Errorf("Client.GetTableMetadata() error = %v, wantErr = %v", err, tt.wantErr)
				return
			}

			if !tt.wantErr && (got.Name != tt.want.Name) {
				t.Errorf("Client.GetTableMetadata() = %v, want = %v", got, tt.want)
			}
		})
	}
}

func TestClient_CreateTable(t *testing.T) {
	tableID := "table-create"
	datasetID := "dataset-table-create"
	ds := types.NewDataset(datasetID)

	client, teardown := mustSetUpTest(t, server.StructSource(types.NewProject(projectID, ds)))
	defer teardown()
	c := &Client{
		Client: client,
	}

	tests := []struct {
		name    string
		schema  string
		dt      *api.Datatype
		wantErr bool
	}{
		{
			name:   "success",
			schema: "./testdata/schema.json",
			dt: &api.Datatype{
				Name:       tableID,
				Experiment: datasetID,
			},
			wantErr: false,
		},
		{
			name: "error",
			dt: &api.Datatype{
				Name:       tableID,
				Experiment: datasetID,
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.schema != "" {
				s, err := os.ReadFile(tt.schema)
				testingx.Must(t, err, "clould not read schema file")
				tt.dt.Schema = s
			}

			err := c.CreateTable(context.Background(), c.Dataset(datasetID), tt.dt)
			if (err != nil) != tt.wantErr {
				t.Errorf("Client.CreateTable() error = %v, wantErr = %v", err, tt.wantErr)
			}
		})
	}
}

func TestClient_UpdateSchema(t *testing.T) {
	tableID := "table-update-schema"
	datasetID := "dataset-update-schema"
	table := types.NewTable(
		tableID,
		[]*types.Column{
			types.NewColumn("id", types.INTEGER),
			types.NewColumn("name", types.STRING),
		},
		types.Data{},
	)
	ds := types.NewDataset(datasetID, table)

	client, teardown := mustSetUpTest(t, server.StructSource(types.NewProject(projectID, ds)))
	defer teardown()
	c := &Client{
		Client: client,
	}

	tests := []struct {
		name    string
		schema  string
		dt      *api.Datatype
		wantErr bool
	}{
		{
			name:   "success",
			schema: "./testdata/schema.json",
			dt: &api.Datatype{
				Name:       tableID,
				Experiment: datasetID,
			},
			wantErr: false,
		},
		{
			name:   "error",
			schema: "./testdata/invalid-schema.json",
			dt: &api.Datatype{
				Name:       tableID,
				Experiment: datasetID,
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s, err := os.ReadFile(tt.schema)
			testingx.Must(t, err, "clould not read schema file")
			tt.dt.Schema = s

			err = c.UpdateSchema(context.Background(), c.Dataset(datasetID), tt.dt)
			if (err != nil) != tt.wantErr {
				t.Errorf("Client.UpdateSchema() error = %v, wantErr = %v", err, tt.wantErr)
			}
		})
	}
}
