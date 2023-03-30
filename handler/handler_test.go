package handler

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"cloud.google.com/go/bigquery"
	"github.com/googleapis/google-cloud-go-testing/bigquery/bqiface"
	"github.com/m-lab/autoloader/api"
	"github.com/m-lab/autoloader/gcs"
	"github.com/m-lab/go/cloudtest/bqfake"
	"github.com/m-lab/go/testingx"
)

type fakeStorage struct {
	datatypes []*api.Datatype
	dirs      map[string][]gcs.Dir
}

func (s *fakeStorage) GetDatatypes(ctx context.Context) []*api.Datatype {
	return s.datatypes
}

func (s *fakeStorage) GetDirs(ctx context.Context, dt *api.Datatype, start, end string) ([]gcs.Dir, error) {
	dir, ok := s.dirs[dt.Name]
	if !ok {
		return nil, errors.New("failed to get dirs")
	}
	return dir, nil
}

type fakeBQ struct {
	datasets     map[string]*bqfake.Dataset
	tables       map[string]*bigquery.TableMetadata
	createDsErr  error
	createTblErr error
	updateErr    error
	loadErr      error
	createCount  int
	updateCount  int
	loadCount    int
}

func (bq *fakeBQ) GetDataset(ctx context.Context, name string) (bqiface.Dataset, error) {
	ds, ok := bq.datasets[name]
	if !ok {
		return nil, errors.New("failed to get dataset")
	}
	return ds, nil
}

func (bq *fakeBQ) GetTableMetadata(ctx context.Context, ds bqiface.Dataset, name string) (*bigquery.TableMetadata, error) {
	tbl, ok := bq.tables[name]
	if !ok {
		return nil, errors.New("failed to get table metadata")
	}
	return tbl, nil
}

func (bq *fakeBQ) CreateDataset(ctx context.Context, dt *api.Datatype) (bqiface.Dataset, error) {
	if bq.createDsErr != nil {
		return nil, bq.createDsErr
	}
	bq.createCount++
	return bqfake.Dataset{}, nil
}

func (bq *fakeBQ) CreateTable(ctx context.Context, ds bqiface.Dataset, dt *api.Datatype) (*bigquery.TableMetadata, error) {
	if bq.createTblErr != nil {
		return nil, bq.createTblErr
	}
	bq.createCount++
	return &bigquery.TableMetadata{}, nil
}

func (bq *fakeBQ) UpdateSchema(ctx context.Context, ds bqiface.Dataset, dt *api.Datatype) error {
	if bq.updateErr != nil {
		return bq.updateErr
	}
	bq.updateCount++
	return nil
}

func (bq *fakeBQ) Load(ctx context.Context, ds bqiface.Dataset, name string, uri ...string) error {
	if bq.loadErr != nil {
		return bq.loadErr
	}
	bq.loadCount++
	return nil
}

func TestClient_Load(t *testing.T) {
	tests := []struct {
		name    string
		storage *fakeStorage
		bq      *fakeBQ
		opts    string
		want    int
	}{
		{
			name: "success",
			storage: &fakeStorage{
				datatypes: []*api.Datatype{{
					Name: "datatype",
				}},
				dirs: map[string][]gcs.Dir{
					"datatype": {{
						Path: "fake-dir-path",
					}},
				},
			},
			bq:   &fakeBQ{},
			opts: "period=day",
			want: http.StatusOK,
		},
		{
			name:    "invalid-opts",
			storage: &fakeStorage{},
			bq:      &fakeBQ{},
			opts:    "period=invalid",
			want:    http.StatusBadRequest,
		},
		{
			name: "processing-error",
			storage: &fakeStorage{
				datatypes: []*api.Datatype{{
					Name: "datatype",
				}},
				dirs: map[string][]gcs.Dir{
					"datatype": {{
						Path: "fake-dir-path",
					}},
				},
			},
			bq:   &fakeBQ{loadErr: errors.New("failed to load data")},
			opts: "period=day",
			want: http.StatusInternalServerError,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := NewClient(tt.storage, tt.bq)
			srv := httptest.NewServer(http.HandlerFunc(c.Load))
			resp, err := http.Get(srv.URL + "?" + tt.opts)
			testingx.Must(t, err, "failed to get test request")

			if resp.StatusCode != tt.want {
				t.Errorf("Handler.Load() status = %d, want %d", resp.StatusCode, tt.want)
			}
		})
	}
}

func TestClient_processDatatype(t *testing.T) {
	tests := []struct {
		name       string
		storage    *fakeStorage
		bq         *fakeBQ
		dt         *api.Datatype
		wantCreate int
		wantUpdate int
		wantLoad   int
		wantErr    bool
	}{
		{
			name: "success-create",
			storage: &fakeStorage{
				dirs: map[string][]gcs.Dir{
					"datatype": {{
						Path: "fake-dir-path",
					}},
				},
			},
			bq: &fakeBQ{},
			dt: &api.Datatype{
				Name: "datatype",
			},
			wantCreate: 2,
			wantUpdate: 0,
			wantLoad:   1,
			wantErr:    false,
		},
		{
			name: "success-exisits-nothing-to-load",
			storage: &fakeStorage{
				dirs: map[string][]gcs.Dir{
					"datatype": {},
				},
			},
			bq: &fakeBQ{
				datasets: map[string]*bqfake.Dataset{"dataset": bqfake.NewDataset(nil, nil, nil)},
				tables:   map[string]*bigquery.TableMetadata{"datatype": {}},
			},
			dt: &api.Datatype{
				Name:       "datatype",
				Experiment: "dataset",
			},
			wantCreate: 0,
			wantUpdate: 0,
			wantLoad:   0,
			wantErr:    false,
		},
		{
			name: "success-exists-update-schema",
			storage: &fakeStorage{
				dirs: map[string][]gcs.Dir{
					"datatype": {{
						Path: "fake-dir-path",
					}},
				},
			},
			bq: &fakeBQ{
				datasets: map[string]*bqfake.Dataset{"dataset": bqfake.NewDataset(nil, nil, nil)},
				tables: map[string]*bigquery.TableMetadata{"datatype": {
					LastModifiedTime: time.Now().Add(-time.Hour),
				}},
			},
			dt: &api.Datatype{
				Name:        "datatype",
				Experiment:  "dataset",
				UpdatedTime: time.Now().Add(-time.Hour),
			},
			wantCreate: 0,
			wantUpdate: 1,
			wantLoad:   1,
			wantErr:    false,
		},
		{
			name: "create-dataset-error",
			storage: &fakeStorage{
				dirs: map[string][]gcs.Dir{
					"datatype": {},
				},
			},
			bq: &fakeBQ{createDsErr: errors.New("failed to create dataset")},
			dt: &api.Datatype{
				Name: "datatype",
			},
			wantCreate: 0,
			wantUpdate: 0,
			wantLoad:   0,
			wantErr:    true,
		},
		{
			name: "create-table-error",
			storage: &fakeStorage{
				dirs: map[string][]gcs.Dir{
					"datatype": {},
				},
			},
			bq: &fakeBQ{createTblErr: errors.New("failed to create table")},
			dt: &api.Datatype{
				Name: "datatype",
			},
			wantCreate: 1,
			wantUpdate: 0,
			wantLoad:   0,
			wantErr:    true,
		},
		{
			name: "update-schema-error",
			storage: &fakeStorage{
				dirs: map[string][]gcs.Dir{},
			},
			bq: &fakeBQ{
				datasets: map[string]*bqfake.Dataset{"dataset": bqfake.NewDataset(nil, nil, nil)},
				tables: map[string]*bigquery.TableMetadata{"datatype": {
					LastModifiedTime: time.Now().Add(-time.Hour),
				}},
				updateErr: errors.New("failed to update schema"),
			},
			dt: &api.Datatype{
				Name:        "datatype",
				Experiment:  "dataset",
				UpdatedTime: time.Now().Add(-time.Hour),
			},
			wantCreate: 0,
			wantUpdate: 0,
			wantLoad:   0,
			wantErr:    true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := NewClient(tt.storage, tt.bq)

			if err := c.processDatatype(context.Background(), tt.dt, periodOpts("all")); (err != nil) != tt.wantErr {
				t.Errorf("Client.processDatatype() error = %v, wantErr = %v", err, tt.wantErr)
			}

			if tt.bq.createCount != tt.wantCreate {
				t.Errorf("Client.processDatatype() create got = %d, want = %d", tt.bq.createCount, tt.wantCreate)
			}

			if tt.bq.updateCount != tt.wantUpdate {
				t.Errorf("Client.processDatatype() update got = %d, want = %d", tt.bq.updateCount, tt.wantUpdate)
			}

			if tt.bq.loadCount != tt.wantLoad {
				t.Errorf("Client.processDatatype() load got = %d, want = %d", tt.bq.loadCount, tt.wantLoad)
			}
		})
	}
}

func TestClient_load(t *testing.T) {
	tests := []struct {
		name     string
		storage  *fakeStorage
		bq       *fakeBQ
		dt       *api.Datatype
		wantLoad int
		wantErr  bool
	}{
		{
			name: "success",
			storage: &fakeStorage{
				dirs: map[string][]gcs.Dir{
					"datatype": {
						{Path: "fake-dir-path1"},
						{Path: "fake-dir-path2"},
						{Path: "fake-dir-path3"},
					},
				},
			},
			bq: &fakeBQ{},
			dt: &api.Datatype{
				Name: "datatype",
			},
			wantLoad: 3,
			wantErr:  false,
		},
		{
			name: "storage-error",
			storage: &fakeStorage{
				dirs: map[string][]gcs.Dir{},
			},
			bq: &fakeBQ{},
			dt: &api.Datatype{
				Name: "datatype",
			},
			wantLoad: 0,
			wantErr:  true,
		},
		{
			name: "load-error",
			storage: &fakeStorage{
				dirs: map[string][]gcs.Dir{
					"datatype": {{
						Path: "fake-dir-path",
					}},
				},
			},
			bq: &fakeBQ{loadErr: errors.New("failed to load file")},
			dt: &api.Datatype{
				Name: "datatype",
			},
			wantLoad: 0,
			wantErr:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := NewClient(tt.storage, tt.bq)

			if err := c.load(context.Background(), nil, tt.dt, periodOpts("all")); (err != nil) != tt.wantErr {
				t.Errorf("Client.load() error = %v, wantErr = %v", err, tt.wantErr)
			}

			if tt.bq.loadCount != tt.wantLoad {
				t.Errorf("Client.load() got = %d, want = %d", tt.bq.loadCount, tt.wantLoad)
			}
		})
	}
}
