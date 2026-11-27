package handler

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"cloud.google.com/go/bigquery"
	"github.com/google/go-cmp/cmp"
	"github.com/googleapis/google-cloud-go-testing/bigquery/bqiface"
	"github.com/m-lab/autoloader/api"
	apiv3 "github.com/m-lab/autoloader/api/v3"
	"github.com/m-lab/autoloader/gcs"
	gcsv3 "github.com/m-lab/autoloader/gcs/v3"
	"github.com/m-lab/autoloader/metrics"
	"github.com/m-lab/go/cloudtest/bqfake"
	"github.com/m-lab/go/testingx"
	"github.com/prometheus/client_golang/prometheus/testutil"
)

const (
	testPrefix = "gs://bucket/autoload/v3"
	part18     = "ndt7_raw$20261118"
	part19     = "ndt7_raw$20261119"
)

var (
	day18   = time.Date(2026, 11, 18, 0, 0, 0, 0, time.UTC)
	day19   = time.Date(2026, 11, 19, 0, 0, 0, 0, time.UTC)
	errFake = errors.New("fake error")
)

// fakeStorageV3 is a StorageClientV3 that returns scripted results.
type fakeStorageV3 struct {
	datatypes []*apiv3.Datatype
	dirs      []gcsv3.Dir
	err       error
}

func (s *fakeStorageV3) GetDatatypes(ctx context.Context) []*apiv3.Datatype {
	return s.datatypes
}

func (s *fakeStorageV3) GetDirs(ctx context.Context, dt *apiv3.Datatype, start, end string) ([]gcsv3.Dir, error) {
	return s.dirs, s.err
}

// fakeBQV3 is a BQClientV3 that returns scripted results and records the
// loads requested from it.
type fakeBQV3 struct {
	datasets     map[string]bool                    // Existing datasets.
	tables       map[string]*bigquery.TableMetadata // Metadata of existing tables and partitions.
	createDsErr  error
	createTblErr error
	updateErr    error
	loadErrs     map[string]error // Load errors by destination table.
	createCount  int
	updateCount  int
	calls        []loadCall
}

// loadCall records one load requested from the fake.
type loadCall struct {
	table  string   // Destination table.
	prefix string   // Hive prefix of the datatype.
	uris   []string // Source URIs.
}

func (bq *fakeBQV3) GetDataset(ctx context.Context, name string) (bqiface.Dataset, error) {
	if !bq.datasets[name] {
		return nil, errors.New("failed to get dataset")
	}
	return bqfake.NewDataset(map[string]*bqfake.Table{}, nil, nil), nil
}

func (bq *fakeBQV3) CreateDataset(ctx context.Context, dt *api.Datatype) (bqiface.Dataset, error) {
	if bq.createDsErr != nil {
		return nil, bq.createDsErr
	}
	bq.createCount++
	return bqfake.NewDataset(map[string]*bqfake.Table{}, nil, nil), nil
}

func (bq *fakeBQV3) GetTableMetadata(ctx context.Context, ds bqiface.Dataset, name string) (*bigquery.TableMetadata, error) {
	md, ok := bq.tables[name]
	if !ok {
		return nil, errors.New("failed to get table metadata")
	}
	return md, nil
}

func (bq *fakeBQV3) CreateTableV3(ctx context.Context, ds bqiface.Dataset, dt *apiv3.Datatype) (*bigquery.TableMetadata, error) {
	if bq.createTblErr != nil {
		return nil, bq.createTblErr
	}
	bq.createCount++
	return &bigquery.TableMetadata{}, nil
}

func (bq *fakeBQV3) UpdateSchemaV3(ctx context.Context, ds bqiface.Dataset, dt *apiv3.Datatype) error {
	if bq.updateErr != nil {
		return bq.updateErr
	}
	bq.updateCount++
	return nil
}

func (bq *fakeBQV3) LoadHive(ctx context.Context, ds bqiface.Dataset, name string, dt *apiv3.Datatype, uri ...string) error {
	bq.calls = append(bq.calls, loadCall{table: name, prefix: dt.HivePrefix, uris: uri})
	return bq.loadErrs[name]
}

// loadErrs returns a fake whose loads fail for the input tables.
func loadErrs(errs map[string]error) *fakeBQV3 {
	return &fakeBQV3{loadErrs: errs}
}

// withTables returns a fake with the input table and partition metadata.
func withTables(tables map[string]*bigquery.TableMetadata) *fakeBQV3 {
	return &fakeBQV3{tables: tables}
}

// v3Datatype returns a v3 datatype for ndt7 in the input experiment.
func v3Datatype(experiment string) *apiv3.Datatype {
	return apiv3.NewDatatype(api.DatatypeOpts{Name: "ndt7", Experiment: experiment, Version: "v3"}, testPrefix)
}

// v3Dir returns the directory of one org for one date.
func v3Dir(org string, date time.Time, newest time.Time) gcsv3.Dir {
	return gcsv3.Dir{
		Path:   testPrefix + "/org=" + org + "/ndt/ndt7/" + date.Format("2006/01/02") + "/*",
		Date:   date,
		Org:    org,
		Newest: newest,
	}
}

func path(org string, date time.Time) string {
	return v3Dir(org, date, time.Time{}).Path
}

func TestClientV3_load(t *testing.T) {
	// T is the last-modified time of an already loaded partition.
	T := time.Date(2026, 11, 20, 12, 0, 0, 0, time.UTC)
	loaded := func(name string) map[string]*bigquery.TableMetadata {
		return map[string]*bigquery.TableMetadata{name: {NumRows: 10, LastModifiedTime: T}}
	}

	tests := []struct {
		name    string
		dirs    []gcsv3.Dir
		bq      *fakeBQV3
		opts    *LoadOptions
		want    []loadCall
		wantErr bool
	}{
		{
			name: "two-dates-two-orgs",
			// Out of order on purpose: dates ascend and orgs are sorted.
			dirs: []gcsv3.Dir{v3Dir("b", day18, T), v3Dir("a", day19, T), v3Dir("a", day18, T)},
			bq:   &fakeBQV3{},
			want: []loadCall{
				{prefix: testPrefix, table: part18, uris: []string{path("a", day18), path("b", day18)}},
				{prefix: testPrefix, table: part19, uris: []string{path("a", day19)}},
			},
		},
		{
			// No retry and no other job: the partition is left as it was.
			name:    "load-job-fails",
			dirs:    []gcsv3.Dir{v3Dir("a", day18, T), v3Dir("b", day18, T)},
			bq:      loadErrs(map[string]error{part18: errFake}),
			want:    []loadCall{{prefix: testPrefix, table: part18, uris: []string{path("a", day18), path("b", day18)}}},
			wantErr: true,
		},
		{
			name: "a-later-date-still-loads",
			dirs: []gcsv3.Dir{v3Dir("a", day18, T), v3Dir("b", day18, T), v3Dir("a", day19, T)},
			bq:   loadErrs(map[string]error{part18: errFake}),
			want: []loadCall{
				{prefix: testPrefix, table: part18, uris: []string{path("a", day18), path("b", day18)}},
				{prefix: testPrefix, table: part19, uris: []string{path("a", day19)}},
			},
			wantErr: true,
		},
		{
			name: "no-dirs",
			dirs: []gcsv3.Dir{},
			bq:   &fakeBQV3{},
			want: nil,
		},
		{
			name: "unchanged-date-is-skipped",
			dirs: []gcsv3.Dir{v3Dir("a", day18, T.Add(-7*time.Hour))},
			bq:   withTables(loaded(part18)),
			want: nil,
		},
		{
			name: "new-object",
			dirs: []gcsv3.Dir{v3Dir("a", day18, T.Add(time.Minute))},
			bq:   withTables(loaded(part18)),
			want: []loadCall{{prefix: testPrefix, table: part18, uris: []string{path("a", day18)}}},
		},
		{
			name: "object-inside-the-margin",
			dirs: []gcsv3.Dir{v3Dir("a", day18, T.Add(-time.Hour))},
			bq:   withTables(loaded(part18)),
			want: []loadCall{{prefix: testPrefix, table: part18, uris: []string{path("a", day18)}}},
		},
		{
			name: "never-loaded",
			dirs: []gcsv3.Dir{v3Dir("a", day18, T.Add(-7*time.Hour))},
			bq: withTables(map[string]*bigquery.TableMetadata{
				part18: {NumRows: 0, LastModifiedTime: T},
			}),
			want: []loadCall{{prefix: testPrefix, table: part18, uris: []string{path("a", day18)}}},
		},
		{
			name: "metadata-fails",
			dirs: []gcsv3.Dir{v3Dir("a", day18, T.Add(-7*time.Hour))},
			// No metadata for the partition: the fake returns an error.
			bq:   withTables(map[string]*bigquery.TableMetadata{}),
			want: []loadCall{{prefix: testPrefix, table: part18, uris: []string{path("a", day18)}}},
		},
		{
			name: "explicit-range-forces",
			dirs: []gcsv3.Dir{v3Dir("a", day18, T.Add(-7*time.Hour))},
			bq:   withTables(loaded(part18)),
			opts: &LoadOptions{"2026/11/18", "2026/11/19", "custom"},
			want: []loadCall{{prefix: testPrefix, table: part18, uris: []string{path("a", day18)}}},
		},
		{
			name: "newest-across-orgs",
			dirs: []gcsv3.Dir{v3Dir("a", day18, T.Add(-48*time.Hour)), v3Dir("b", day18, T.Add(time.Minute))},
			bq:   withTables(loaded(part18)),
			want: []loadCall{{prefix: testPrefix, table: part18, uris: []string{path("a", day18), path("b", day18)}}},
		},
		{
			name: "only-the-changed-date-loads",
			dirs: []gcsv3.Dir{v3Dir("a", day18, T.Add(-7*time.Hour)), v3Dir("a", day19, T.Add(time.Minute))},
			bq: withTables(map[string]*bigquery.TableMetadata{
				part18: {NumRows: 10, LastModifiedTime: T},
				part19: {NumRows: 10, LastModifiedTime: T},
			}),
			want: []loadCall{{prefix: testPrefix, table: part19, uris: []string{path("a", day19)}}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// A separate experiment per case keeps the counters apart.
			dt := v3Datatype(tt.name)
			storage := &fakeStorageV3{dirs: tt.dirs}
			if tt.bq.tables == nil {
				// Unless a case says otherwise, the partitions have never been
				// loaded: their metadata reports zero rows.
				tt.bq.tables = map[string]*bigquery.TableMetadata{part18: {}, part19: {}}
			}
			c := NewClientV3(storage, tt.bq)
			ds := bqfake.NewDataset(map[string]*bqfake.Table{}, nil, nil)
			opts := tt.opts
			if opts == nil {
				opts = periodOpts("daily")
			}

			err := c.load(context.Background(), ds, dt, opts)
			if (err != nil) != tt.wantErr {
				t.Errorf("ClientV3.load() error = %v, wantErr = %v", err, tt.wantErr)
			}

			if diff := cmp.Diff(tt.want, tt.bq.calls, cmp.AllowUnexported(loadCall{})); diff != "" {
				t.Errorf("ClientV3.load() calls mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

func TestClientV3_load_ReturnsLastError(t *testing.T) {
	// Both dates fail: the error of the last one is returned.
	errLast := errors.New("last")
	bq := loadErrs(map[string]error{part18: errFake, part19: errLast})
	storage := &fakeStorageV3{dirs: []gcsv3.Dir{
		v3Dir("a", day18, time.Time{}), v3Dir("a", day19, time.Time{}),
	}}
	c := NewClientV3(storage, bq)
	ds := bqfake.NewDataset(map[string]*bqfake.Table{}, nil, nil)

	err := c.load(context.Background(), ds, v3Datatype("last-error"), periodOpts("daily"))
	if !errors.Is(err, errLast) {
		t.Errorf("ClientV3.load() error = %v, want = %v", err, errLast)
	}
}

func TestClientV3_load_Cancelled(t *testing.T) {
	// The request is cancelled during the load of the 18th: the failure is not
	// counted as a load error and the 19th is not attempted.
	ctx, cancel := context.WithCancel(context.Background())
	bq := &cancellingBQ{fakeBQV3: &fakeBQV3{}, cancel: cancel}
	storage := &fakeStorageV3{dirs: []gcsv3.Dir{
		v3Dir("a", day18, time.Time{}), v3Dir("a", day19, time.Time{}),
	}}
	c := NewClientV3(storage, bq)
	ds := bqfake.NewDataset(map[string]*bqfake.Table{}, nil, nil)

	err := c.load(ctx, ds, v3Datatype("cancelled"), periodOpts("daily"))
	if !errors.Is(err, context.Canceled) {
		t.Errorf("ClientV3.load() error = %v, want = %v", err, context.Canceled)
	}
	if len(bq.calls) != 1 || bq.calls[0].table != part18 {
		t.Errorf("ClientV3.load() calls = %v, want only a load of %s", bq.calls, part18)
	}
	got := testutil.ToFloat64(metrics.LoadErrorsTotal.WithLabelValues("v3", "cancelled", "ndt7", unknown, unknown))
	if got != 0 {
		t.Errorf("LoadErrorsTotal got = %v, want = 0", got)
	}
}

// cancellingBQ cancels the request during the first load, which then fails
// like every call made with a cancelled context.
type cancellingBQ struct {
	*fakeBQV3
	cancel context.CancelFunc
}

func (bq *cancellingBQ) LoadHive(ctx context.Context, ds bqiface.Dataset, name string, dt *apiv3.Datatype, uri ...string) error {
	bq.calls = append(bq.calls, loadCall{table: name, prefix: dt.HivePrefix, uris: uri})
	bq.cancel()
	return context.Canceled
}

func TestClientV3_load_StorageError(t *testing.T) {
	bq := &fakeBQV3{}
	c := NewClientV3(&fakeStorageV3{err: errFake}, bq)
	err := c.load(context.Background(), nil, v3Datatype("storage-error"), periodOpts("daily"))
	if err == nil {
		t.Error("ClientV3.load() error = nil, wantErr = true")
	}
	if len(bq.calls) != 0 {
		t.Errorf("ClientV3.load() calls got = %v, want none", bq.calls)
	}
}

func TestClientV3_load_LoadedDates(t *testing.T) {
	// The 18th fails (error) and the 19th loads (OK).
	dt := v3Datatype("loaded-dates")
	bq := loadErrs(map[string]error{part18: errFake})
	storage := &fakeStorageV3{dirs: []gcsv3.Dir{
		v3Dir("a", day18, time.Time{}), v3Dir("b", day18, time.Time{}), v3Dir("a", day19, time.Time{}),
	}}
	c := NewClientV3(storage, bq)
	ds := bqfake.NewDataset(map[string]*bqfake.Table{}, nil, nil)

	if err := c.load(context.Background(), ds, dt, periodOpts("daily")); err == nil {
		t.Error("ClientV3.load() error = nil, wantErr = true")
	}

	got := testutil.ToFloat64(metrics.LoadedDates.WithLabelValues("v3", "loaded-dates", "ndt7", "daily", "error"))
	if got != float64(day18.Unix()) {
		t.Errorf("LoadedDates error got = %v, want = %v", got, day18.Unix())
	}
	got = testutil.ToFloat64(metrics.LoadedDates.WithLabelValues("v3", "loaded-dates", "ndt7", "daily", "OK"))
	if got != float64(day19.Unix()) {
		t.Errorf("LoadedDates OK got = %v, want = %v", got, day19.Unix())
	}
}

// bqErr returns a BigQuery job error as jobErrors in the bq package joins them.
func bqErr(errs ...*bigquery.Error) error {
	var err error
	for _, e := range errs {
		err = errors.Join(err, e)
	}
	return err
}

func TestClientV3_load_LoadErrors(t *testing.T) {
	bad := "gs://bucket/autoload/v3/org=test-bad/ndt/ndt7/2026/11/18/bad.jsonl.gz"
	worse := "gs://bucket/autoload/v3/org=foo-bar/ndt/ndt7/2026/11/18/worse.jsonl.gz"

	tests := []struct {
		name string
		err  error
		want []loadFailure
	}{
		{
			// What BigQuery returns for a row with an unknown field: a summary
			// and a detail error, both naming the file.
			name: "one-org-named-twice",
			err: bqErr(
				&bigquery.Error{Location: bad, Message: "JSON table encountered too many errors, giving up. File: " + bad, Reason: "invalid"},
				&bigquery.Error{Location: bad, Message: "No such field: nosuchfield. File: " + bad, Reason: "invalid"},
			),
			want: []loadFailure{{"test-bad", "invalid"}},
		},
		{
			name: "two-orgs",
			err: bqErr(
				&bigquery.Error{Location: bad, Reason: "invalid"},
				&bigquery.Error{Location: worse, Reason: "invalid"},
			),
			want: []loadFailure{{"test-bad", "invalid"}, {"foo-bar", "invalid"}},
		},
		{
			name: "file-only-in-message",
			err:  bqErr(&bigquery.Error{Message: "Error while reading data. File: " + bad, Reason: "invalid"}),
			want: []loadFailure{{"test-bad", "invalid"}},
		},
		{
			// A row whose date differs from the partition's names no file.
			name: "no-file-named",
			err: bqErr(&bigquery.Error{
				Message: "Some rows belong to different partitions rather than destination partition",
				Reason:  "invalid",
			}),
			want: []loadFailure{{unknown, "invalid"}},
		},
		{
			name: "backend-error",
			err:  bqErr(&bigquery.Error{Message: "An internal error occurred", Reason: "backendError"}),
			want: []loadFailure{{unknown, "backendError"}},
		},
		{
			// An error without a file next to one with a file: only the org
			// is counted.
			name: "named-and-unnamed",
			err: bqErr(
				&bigquery.Error{Message: "too many errors", Reason: "invalid"},
				&bigquery.Error{Location: bad, Reason: "invalid"},
			),
			want: []loadFailure{{"test-bad", "invalid"}},
		},
		{
			name: "wrapped",
			err:  fmt.Errorf("load failed: %w", bqErr(&bigquery.Error{Location: bad, Reason: "invalid"})),
			want: []loadFailure{{"test-bad", "invalid"}},
		},
		{
			name: "not-a-bigquery-error",
			err:  errFake,
			want: []loadFailure{{unknown, unknown}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// A separate experiment per case keeps the counters apart.
			dt := v3Datatype("load-errors-" + tt.name)
			bq := loadErrs(map[string]error{part18: tt.err})
			storage := &fakeStorageV3{dirs: []gcsv3.Dir{v3Dir("a", day18, time.Time{})}}
			c := NewClientV3(storage, bq)
			ds := bqfake.NewDataset(map[string]*bqfake.Table{}, nil, nil)

			if err := c.load(context.Background(), ds, dt, periodOpts("daily")); err == nil {
				t.Error("ClientV3.load() error = nil, wantErr = true")
			}

			if got := loadFailures(tt.err); !cmp.Equal(got, tt.want, cmp.AllowUnexported(loadFailure{})) {
				t.Errorf("loadFailures() = %v, want = %v", got, tt.want)
			}
			for _, f := range tt.want {
				got := testutil.ToFloat64(metrics.LoadErrorsTotal.WithLabelValues("v3", dt.Experiment, "ndt7", f.org, f.reason))
				if got != 1 {
					t.Errorf("LoadErrorsTotal{org=%q, reason=%q} got = %v, want = 1", f.org, f.reason, got)
				}
			}
		})
	}
}

func TestClientV3_processDatatype(t *testing.T) {
	dirs := []gcsv3.Dir{v3Dir("a", day18, time.Time{}), v3Dir("a", day19, time.Time{})}
	existing := func() *fakeBQV3 {
		return &fakeBQV3{
			datasets: map[string]bool{"autoload_v3_process": true},
			// The schema file is older than the table: the schema is updated anyway.
			tables: map[string]*bigquery.TableMetadata{"ndt7_raw": {LastModifiedTime: time.Now()}},
		}
	}

	tests := []struct {
		name       string
		bq         *fakeBQV3
		wantCreate int
		wantUpdate int
		wantLoads  int
		wantErr    bool
	}{
		{name: "create-and-load", bq: &fakeBQV3{}, wantCreate: 2, wantUpdate: 1, wantLoads: 2},
		{name: "exists-schema-always-updated", bq: existing(), wantUpdate: 1, wantLoads: 2},
		{name: "create-dataset-error", bq: &fakeBQV3{createDsErr: errFake}, wantErr: true},
		{name: "create-table-error", bq: &fakeBQV3{createTblErr: errFake}, wantCreate: 1, wantErr: true},
		{
			name: "update-schema-error",
			bq: func() *fakeBQV3 {
				bq := existing()
				bq.updateErr = errFake
				return bq
			}(),
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dt := v3Datatype("process")
			dt.UpdatedTime = time.Now().Add(-time.Hour)
			c := NewClientV3(&fakeStorageV3{dirs: dirs}, tt.bq)

			err := c.processDatatype(context.Background(), dt, periodOpts("daily"))
			if (err != nil) != tt.wantErr {
				t.Errorf("ClientV3.processDatatype() error = %v, wantErr = %v", err, tt.wantErr)
			}
			if tt.bq.createCount != tt.wantCreate {
				t.Errorf("ClientV3.processDatatype() create got = %d, want = %d", tt.bq.createCount, tt.wantCreate)
			}
			if tt.bq.updateCount != tt.wantUpdate {
				t.Errorf("ClientV3.processDatatype() update got = %d, want = %d", tt.bq.updateCount, tt.wantUpdate)
			}
			if len(tt.bq.calls) != tt.wantLoads {
				t.Errorf("ClientV3.processDatatype() loads got = %d, want = %d", len(tt.bq.calls), tt.wantLoads)
			}
		})
	}
}

func TestClientV3_Load(t *testing.T) {
	tests := []struct {
		name string
		bq   *fakeBQV3
		opts string
		want int
	}{
		{name: "success", bq: &fakeBQV3{}, opts: "period=daily", want: http.StatusOK},
		{name: "invalid-opts", bq: &fakeBQV3{}, opts: "period=invalid", want: http.StatusBadRequest},
		{
			name: "load-error",
			bq:   loadErrs(map[string]error{part18: errFake}),
			opts: "period=daily",
			want: http.StatusInternalServerError,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			storage := &fakeStorageV3{
				datatypes: []*apiv3.Datatype{v3Datatype("load-" + tt.name)},
				dirs:      []gcsv3.Dir{v3Dir("a", day18, time.Time{})},
			}
			c := NewClientV3(storage, tt.bq)
			srv := httptest.NewServer(http.HandlerFunc(c.Load))
			defer srv.Close()
			resp, err := http.Get(srv.URL + "?" + tt.opts)
			testingx.Must(t, err, "failed to get test request")

			if resp.StatusCode != tt.want {
				t.Errorf("ClientV3.Load() status = %d, want %d", resp.StatusCode, tt.want)
			}
		})
	}
}

func TestVersionLabel(t *testing.T) {
	// A v1, a v2 and a v3 datatype with the same experiment and name do not
	// share a series. A datatype without a version counts as v1.
	const experiment = "version-label"
	for _, version := range []string{"", "v2"} {
		dt := api.NewMlabDatatype(api.DatatypeOpts{Name: "ndt7", Experiment: experiment, Version: version})
		storage := &fakeStorage{dirs: map[string][]gcs.Dir{"ndt7": {{Path: "path", Date: day18}}}}
		c := NewClient(storage, &fakeBQ{})
		if err := c.processDatatype(context.Background(), dt, periodOpts("daily")); err != nil {
			t.Fatalf("Client.processDatatype() error = %v, wantErr = false", err)
		}
	}
	v3 := NewClientV3(&fakeStorageV3{dirs: []gcsv3.Dir{v3Dir("a", day18, time.Time{})}}, &fakeBQV3{})
	if err := v3.processDatatype(context.Background(), v3Datatype(experiment), periodOpts("daily")); err != nil {
		t.Fatalf("ClientV3.processDatatype() error = %v, wantErr = false", err)
	}

	for _, version := range []string{"v1", "v2", "v3"} {
		got := testutil.ToFloat64(metrics.BigQueryOperationsTotal.WithLabelValues(version, experiment, "ndt7", "load", "OK"))
		if got != 1 {
			t.Errorf("BigQueryOperationsTotal %s got = %v, want = 1", version, got)
		}
	}
}
