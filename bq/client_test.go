package bq

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"cloud.google.com/go/bigquery"
	"github.com/googleapis/google-cloud-go-testing/bigquery/bqiface"
	"github.com/m-lab/autoloader/api"
	"github.com/m-lab/go/cloudtest/bqfake"
	"github.com/m-lab/go/testingx"
)

var (
	projectID    = "project"
	experimentID = "experiment1"
	datatypeID   = "datatype1"
)

func TestNewClient(t *testing.T) {
	bqMain, err := bigquery.NewClient(context.Background(), "foo")
	testingx.Must(t, err, "failed to create fake BQ client")
	defer bqMain.Close()
	bqView, err := bigquery.NewClient(context.Background(), "bar")
	testingx.Must(t, err, "failed to create fake BQ view client")
	defer bqView.Close()
	got := NewClient(bqMain, bqView)

	want := &Client{
		Client:     bqiface.AdaptClient(bqMain),
		ViewClient: bqiface.AdaptClient(bqView),
	}

	if !reflect.DeepEqual(got, want) {
		t.Fatalf("NewClient() = %v, want = %v", got, want)
	}
}

func TestClient_GetDataset(t *testing.T) {
	ds := bqfake.NewDataset(nil, &bqiface.DatasetMetadata{
		DatasetMetadata: bigquery.DatasetMetadata{
			Name: experimentID,
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
			datasets: map[string]*bqfake.Dataset{experimentID: ds},
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
			bq, err := bqfake.NewClient(context.Background(), projectID, tt.datasets)
			testingx.Must(t, err, "failed to create fake bq client")
			c := &Client{Client: bq}

			got, err := c.GetDataset(context.Background(), experimentID)
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
		dt      *api.Datatype
		wantErr bool
	}{
		{
			name:    "success-mlab",
			dataset: bqfake.NewDataset(nil, nil, nil),
			dt: api.NewMlabDatatype(
				api.DatatypeOpts{
					Experiment: experimentID,
				}),
			wantErr: false,
		},
		{
			name:    "success-third-party",
			dataset: bqfake.NewDataset(nil, nil, nil),
			dt: api.NewThirdPartyDatatype(
				api.DatatypeOpts{
					Experiment: experimentID,
				}, ""),
			wantErr: false,
		},
		{
			name:    "error",
			dataset: bqfake.NewDataset(nil, nil, errors.New("create dataset error")),
			dt: api.NewMlabDatatype(
				api.DatatypeOpts{
					Experiment: experimentID,
				}),
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			bq, err := bqfake.NewClient(context.Background(), projectID, map[string]*bqfake.Dataset{tt.dt.Dataset(): tt.dataset})
			testingx.Must(t, err, "failed to create fake bq client")
			c := &Client{Client: bq}

			got, err := c.CreateDataset(context.Background(), tt.dt)
			if (err != nil) != tt.wantErr {
				t.Fatalf("Client.CreateDataset() error = %v, wantErr = %v", err, tt.wantErr)
			}

			if tt.wantErr {
				return
			}

			if got != tt.dataset {
				t.Errorf("Client.CreateDataset() = %+v, want = %+v", got, tt.dataset)
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
				Name:     datatypeID,
				Metadata: tt.md,
			}
			table := bqfake.NewTable(opts)
			ds := bqfake.NewDataset(map[string]*bqfake.Table{datatypeID: table}, nil, nil)
			bq, err := bqfake.NewClient(context.Background(), projectID, map[string]*bqfake.Dataset{experimentID: ds})
			testingx.Must(t, err, "failed to create fake bq client")
			c := &Client{Client: bq}

			got, err := c.GetTableMetadata(context.Background(), ds, datatypeID)
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
		mdType  bigquery.TableType
		dt      *api.Datatype
		wantErr bool
	}{
		{
			name: "success-mlab",
			dt: api.NewMlabDatatype(
				api.DatatypeOpts{
					Name:       datatypeID,
					Experiment: experimentID,
					Schema:     testingx.MustReadFile(t, "./testdata/schema.json"),
				}),
			wantErr: false,
		},
		{
			name: "success-third-party",
			dt: api.NewThirdPartyDatatype(
				api.DatatypeOpts{
					Name:       datatypeID,
					Experiment: experimentID,
					Schema:     testingx.MustReadFile(t, "./testdata/schema.json"),
				}, ""),
			wantErr: false,
		},
		{
			// TableMetadata.Type != "" indicates the table has been created.
			// If the TYPE is already set, bqfake.Table.Create() returns an error.
			name:   "create-error",
			mdType: "TYPE",
			dt: api.NewMlabDatatype(
				api.DatatypeOpts{
					Name:       datatypeID,
					Experiment: experimentID,
					Schema:     testingx.MustReadFile(t, "./testdata/schema.json"),
				}),
			wantErr: true,
		},
		{
			name: "invalid-schema",
			dt: api.NewMlabDatatype(
				api.DatatypeOpts{
					Name:       datatypeID,
					Experiment: experimentID,
					Schema:     testingx.MustReadFile(t, "./testdata/invalid-schema.json"),
				}),
			wantErr: true,
		},
		{
			name: "no-schema",
			dt: api.NewMlabDatatype(
				api.DatatypeOpts{
					Name:       datatypeID,
					Experiment: experimentID,
				}),
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			md := &bigquery.TableMetadata{Type: tt.mdType}
			opts := bqfake.TableOpts{
				Dataset:  bqfake.Dataset{},
				Name:     tt.dt.Table(),
				Metadata: md,
			}
			table := bqfake.NewTable(opts)
			ds := bqfake.NewDataset(map[string]*bqfake.Table{tt.dt.Table(): table}, nil, nil)
			bq, err := bqfake.NewClient(context.Background(), projectID, map[string]*bqfake.Dataset{tt.dt.Dataset(): ds})
			testingx.Must(t, err, "failed to create fake bq client")
			c := &Client{Client: bq}

			got, err := c.CreateTable(context.Background(), ds, tt.dt)
			if (err != nil) != tt.wantErr {
				t.Fatalf("Client.CreateTable() error = %v, wantErr = %v", err, tt.wantErr)
			}

			if tt.wantErr {
				return
			}

			// The bqfake package sets the Type field on table creation.
			md.Type = "TABLE"
			if got != md {
				t.Errorf("Client.CreateTable() = %v, want = %v", got, md)
			}
		})
	}
}

func TestClient_UpdateSchema(t *testing.T) {
	tests := []struct {
		name      string
		dt        *api.Datatype
		updateErr error
		wantErr   bool
	}{
		{
			name: "success-mlab",
			dt: api.NewMlabDatatype(api.DatatypeOpts{
				Name:   datatypeID,
				Schema: testingx.MustReadFile(t, "./testdata/schema.json"),
			}),
			wantErr: false,
		},
		{
			name: "success-third-party",
			dt: api.NewThirdPartyDatatype(api.DatatypeOpts{
				Name:   datatypeID,
				Schema: testingx.MustReadFile(t, "./testdata/schema.json"),
			}, ""),
			wantErr: false,
		},
		{
			name: "invalid-schema",
			dt: api.NewMlabDatatype(api.DatatypeOpts{
				Name:   datatypeID,
				Schema: testingx.MustReadFile(t, "./testdata/invalid-schema.json"),
			}),
			wantErr: true,
		},
		{
			name: "no-schema",
			dt: api.NewMlabDatatype(api.DatatypeOpts{
				Name: datatypeID,
			}),
			wantErr: true,
		},
		{
			name: "update-err",
			dt: api.NewThirdPartyDatatype(api.DatatypeOpts{
				Name:   datatypeID,
				Schema: testingx.MustReadFile(t, "./testdata/schema.json"),
			}, ""),
			updateErr: errors.New("update error"),
			wantErr:   true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			opts := bqfake.TableOpts{
				Dataset:   bqfake.Dataset{},
				Name:      tt.dt.Table(),
				Metadata:  &bigquery.TableMetadata{},
				UpdateErr: tt.updateErr,
			}
			table := bqfake.NewTable(opts)
			ds := bqfake.NewDataset(map[string]*bqfake.Table{tt.dt.Table(): table}, nil, nil)
			bq, err := bqfake.NewClient(context.Background(), projectID, map[string]*bqfake.Dataset{tt.dt.Dataset(): ds})
			testingx.Must(t, err, "failed to create fake bq client")
			c := &Client{Client: bq, ViewClient: bq}

			err = c.UpdateSchema(context.Background(), ds, tt.dt)
			if (err != nil) != tt.wantErr {
				t.Fatalf("Client.UpdateSchema() error = %v, wantErr = %v", err, tt.wantErr)
			}
		})
	}
}

func TestClient_UpdateSchemaAndView(t *testing.T) {
	dt := api.NewMlabDatatype(api.DatatypeOpts{
		Name:       datatypeID,
		Experiment: experimentID,
		Schema:     testingx.MustReadFile(t, "./testdata/schema.json"),
	})

	opts := bqfake.TableOpts{
		Dataset: bqfake.Dataset{},
		Name:    dt.Table(),
		Metadata: &bigquery.TableMetadata{
			Type: "TABLE",
		},
	}
	table := bqfake.NewTable(opts)
	ds := bqfake.NewDataset(map[string]*bqfake.Table{dt.Table(): table},
		&bqiface.DatasetMetadata{DatasetMetadata: bigquery.DatasetMetadata{Name: dt.Dataset()}}, nil)

	tests := []struct {
		name    string
		viewds  *bqfake.Dataset
		wantErr bool
	}{
		{
			name: "success",
			viewds: bqfake.NewDataset(
				map[string]*bqfake.Table{dt.ViewTable(): bqfake.NewTable(bqfake.TableOpts{
					Dataset: bqfake.Dataset{},
					Name:    dt.ViewTable(),
					Metadata: &bigquery.TableMetadata{
						Type: "VIEW",
					},
				})},
				&bqiface.DatasetMetadata{}, nil),
			wantErr: false,
		},
		{
			name:    "error",
			viewds:  bqfake.NewDataset(nil, nil, nil),
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			bq, err := bqfake.NewClient(context.Background(), projectID, map[string]*bqfake.Dataset{
				dt.Dataset():     ds,
				dt.ViewDataset(): tt.viewds,
			})
			testingx.Must(t, err, "failed to create fake bq client")
			c := Client{Client: bq, ViewClient: bq}

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
