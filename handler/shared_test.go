package handler

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"cloud.google.com/go/bigquery"
	"github.com/google/go-cmp/cmp"
	"github.com/googleapis/google-cloud-go-testing/bigquery/bqiface"
	"github.com/m-lab/autoloader/api"
	apiv3 "github.com/m-lab/autoloader/api/v3"
	"github.com/m-lab/autoloader/gcs"
	"github.com/m-lab/autoloader/metrics"
	"github.com/m-lab/go/cloudtest/bqfake"
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

// sharedDatatype returns a datatype that is loaded into a shared table.
func sharedDatatype(experiment string) *api.Datatype {
	return apiv3.NewDatatype(api.DatatypeOpts{Name: "ndt7", Experiment: experiment, Version: "v3"}, testPrefix)
}

// sharedDir returns the directory of one org for one date.
func sharedDir(org string, date time.Time, newest time.Time) gcs.Dir {
	return gcs.Dir{
		Path:   testPrefix + "/org=" + org + "/ndt/ndt7/" + date.Format("2006/01/02") + "/*",
		Date:   date,
		Org:    org,
		Newest: newest,
	}
}

func path(org string, date time.Time) string {
	return sharedDir(org, date, time.Time{}).Path
}

func TestClient_loadShared(t *testing.T) {
	// T is the last-modified time of an already loaded partition.
	T := time.Date(2026, 11, 20, 12, 0, 0, 0, time.UTC)
	loaded := func(name string) map[string]*bigquery.TableMetadata {
		return map[string]*bigquery.TableMetadata{name: {NumRows: 10, LastModifiedTime: T}}
	}

	tests := []struct {
		name    string
		dirs    []gcs.Dir
		bq      *fakeBQ
		opts    *LoadOptions
		want    []call
		wantErr bool
	}{
		{
			name: "two-dates-two-orgs",
			// Out of order on purpose: dates ascend and orgs are sorted.
			dirs: []gcs.Dir{sharedDir("b", day18, T), sharedDir("a", day19, T), sharedDir("a", day18, T)},
			bq:   &fakeBQ{},
			want: []call{
				{method: "LoadHive", prefix: testPrefix, table: part18, uris: []string{path("a", day18), path("b", day18)}},
				{method: "LoadHive", prefix: testPrefix, table: part19, uris: []string{path("a", day19)}},
			},
		},
		{
			// No retry and no other job: the partition is left as it was.
			name:    "shared-job-fails",
			dirs:    []gcs.Dir{sharedDir("a", day18, T), sharedDir("b", day18, T)},
			bq:      &fakeBQ{hiveErrs: map[string]error{part18: errFake}},
			want:    []call{{method: "LoadHive", prefix: testPrefix, table: part18, uris: []string{path("a", day18), path("b", day18)}}},
			wantErr: true,
		},
		{
			name: "a-later-date-still-loads",
			dirs: []gcs.Dir{sharedDir("a", day18, T), sharedDir("b", day18, T), sharedDir("a", day19, T)},
			bq:   &fakeBQ{hiveErrs: map[string]error{part18: errFake}},
			want: []call{
				{method: "LoadHive", prefix: testPrefix, table: part18, uris: []string{path("a", day18), path("b", day18)}},
				{method: "LoadHive", prefix: testPrefix, table: part19, uris: []string{path("a", day19)}},
			},
			wantErr: true,
		},
		{
			name: "invalid-date-is-skipped",
			dirs: []gcs.Dir{sharedDir("a", time.Time{}, T), sharedDir("a", day18, T)},
			bq:   &fakeBQ{},
			want: []call{{method: "LoadHive", prefix: testPrefix, table: part18, uris: []string{path("a", day18)}}},
		},
		{
			name: "no-dirs",
			dirs: []gcs.Dir{},
			bq:   &fakeBQ{},
			want: nil,
		},
		{
			name: "unchanged-date-is-skipped",
			dirs: []gcs.Dir{sharedDir("a", day18, T.Add(-7*time.Hour))},
			bq:   &fakeBQ{tables: loaded(part18)},
			want: nil,
		},
		{
			name: "new-object",
			dirs: []gcs.Dir{sharedDir("a", day18, T.Add(time.Minute))},
			bq:   &fakeBQ{tables: loaded(part18)},
			want: []call{{method: "LoadHive", prefix: testPrefix, table: part18, uris: []string{path("a", day18)}}},
		},
		{
			name: "object-inside-the-margin",
			dirs: []gcs.Dir{sharedDir("a", day18, T.Add(-time.Hour))},
			bq:   &fakeBQ{tables: loaded(part18)},
			want: []call{{method: "LoadHive", prefix: testPrefix, table: part18, uris: []string{path("a", day18)}}},
		},
		{
			name: "never-loaded",
			dirs: []gcs.Dir{sharedDir("a", day18, T.Add(-7*time.Hour))},
			bq: &fakeBQ{tables: map[string]*bigquery.TableMetadata{
				part18: {NumRows: 0, LastModifiedTime: T},
			}},
			want: []call{{method: "LoadHive", prefix: testPrefix, table: part18, uris: []string{path("a", day18)}}},
		},
		{
			name: "metadata-fails",
			dirs: []gcs.Dir{sharedDir("a", day18, T.Add(-7*time.Hour))},
			// No metadata for the partition: the fake returns an error.
			bq:   &fakeBQ{tables: map[string]*bigquery.TableMetadata{}},
			want: []call{{method: "LoadHive", prefix: testPrefix, table: part18, uris: []string{path("a", day18)}}},
		},
		{
			name: "explicit-range-forces",
			dirs: []gcs.Dir{sharedDir("a", day18, T.Add(-7*time.Hour))},
			bq:   &fakeBQ{tables: loaded(part18)},
			opts: &LoadOptions{"2026/11/18", "2026/11/19", "custom"},
			want: []call{{method: "LoadHive", prefix: testPrefix, table: part18, uris: []string{path("a", day18)}}},
		},
		{
			name: "newest-across-orgs",
			dirs: []gcs.Dir{sharedDir("a", day18, T.Add(-48*time.Hour)), sharedDir("b", day18, T.Add(time.Minute))},
			bq:   &fakeBQ{tables: loaded(part18)},
			want: []call{{method: "LoadHive", prefix: testPrefix, table: part18, uris: []string{path("a", day18), path("b", day18)}}},
		},
		{
			name: "only-the-changed-date-loads",
			dirs: []gcs.Dir{sharedDir("a", day18, T.Add(-7*time.Hour)), sharedDir("a", day19, T.Add(time.Minute))},
			bq: &fakeBQ{tables: map[string]*bigquery.TableMetadata{
				part18: {NumRows: 10, LastModifiedTime: T},
				part19: {NumRows: 10, LastModifiedTime: T},
			}},
			want: []call{{method: "LoadHive", prefix: testPrefix, table: part19, uris: []string{path("a", day19)}}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// A separate experiment per case keeps the counters apart.
			dt := sharedDatatype(tt.name)
			storage := &fakeStorage{dirs: map[string][]gcs.Dir{"ndt7": tt.dirs}}
			if tt.bq.tables == nil {
				// Unless a case says otherwise, the partitions have never been
				// loaded: their metadata reports zero rows.
				tt.bq.tables = map[string]*bigquery.TableMetadata{part18: {}, part19: {}}
			}
			c := NewClient(storage, tt.bq)
			ds := bqfake.NewDataset(map[string]*bqfake.Table{}, nil, nil)
			opts := tt.opts
			if opts == nil {
				opts = periodOpts("daily")
			}

			err := c.loadShared(context.Background(), ds, dt, opts)
			if (err != nil) != tt.wantErr {
				t.Errorf("Client.loadShared() error = %v, wantErr = %v", err, tt.wantErr)
			}

			if diff := cmp.Diff(tt.want, tt.bq.calls, cmp.AllowUnexported(call{})); diff != "" {
				t.Errorf("Client.loadShared() calls mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

func TestClient_loadShared_ReturnsLastError(t *testing.T) {
	// Both dates fail: the error of the last one is returned.
	errLast := errors.New("last")
	bq := &fakeBQ{hiveErrs: map[string]error{part18: errFake, part19: errLast}}
	storage := &fakeStorage{dirs: map[string][]gcs.Dir{"ndt7": {
		sharedDir("a", day18, time.Time{}), sharedDir("a", day19, time.Time{}),
	}}}
	c := NewClient(storage, bq)
	ds := bqfake.NewDataset(map[string]*bqfake.Table{}, nil, nil)

	err := c.loadShared(context.Background(), ds, sharedDatatype("last-error"), periodOpts("daily"))
	if !errors.Is(err, errLast) {
		t.Errorf("Client.loadShared() error = %v, want = %v", err, errLast)
	}
}

func TestClient_loadShared_Cancelled(t *testing.T) {
	// The request is cancelled during the load of the 18th: the failure is not
	// counted as a load error and the 19th is not attempted.
	ctx, cancel := context.WithCancel(context.Background())
	bq := &cancellingBQ{fakeBQ: &fakeBQ{}, cancel: cancel}
	storage := &fakeStorage{dirs: map[string][]gcs.Dir{"ndt7": {
		sharedDir("a", day18, time.Time{}), sharedDir("a", day19, time.Time{}),
	}}}
	c := NewClient(storage, bq)
	ds := bqfake.NewDataset(map[string]*bqfake.Table{}, nil, nil)

	err := c.loadShared(ctx, ds, sharedDatatype("cancelled"), periodOpts("daily"))
	if !errors.Is(err, context.Canceled) {
		t.Errorf("Client.loadShared() error = %v, want = %v", err, context.Canceled)
	}
	if len(bq.calls) != 1 || bq.calls[0].table != part18 {
		t.Errorf("Client.loadShared() calls = %v, want only a load of %s", bq.calls, part18)
	}
	got := testutil.ToFloat64(metrics.LoadErrorsTotal.WithLabelValues("v3", "cancelled", "ndt7", unknown, unknown))
	if got != 0 {
		t.Errorf("LoadErrorsTotal got = %v, want = 0", got)
	}
}

// cancellingBQ cancels the request during the first load, which then fails
// like every call made with a cancelled context.
type cancellingBQ struct {
	*fakeBQ
	cancel context.CancelFunc
}

func (bq *cancellingBQ) LoadHive(ctx context.Context, ds bqiface.Dataset, name string, dt *api.Datatype, uri ...string) error {
	bq.fakeBQ.LoadHive(ctx, ds, name, dt, uri...)
	bq.cancel()
	return context.Canceled
}

func TestClient_loadShared_StorageError(t *testing.T) {
	bq := &fakeBQ{}
	c := NewClient(&fakeStorage{dirs: map[string][]gcs.Dir{}}, bq)
	err := c.loadShared(context.Background(), nil, sharedDatatype("storage-error"), periodOpts("daily"))
	if err == nil {
		t.Error("Client.loadShared() error = nil, wantErr = true")
	}
	if len(bq.calls) != 0 {
		t.Errorf("Client.loadShared() calls got = %v, want none", bq.calls)
	}
}

func TestClient_loadShared_LoadedDates(t *testing.T) {
	// The 18th fails (error) and the 19th loads (OK).
	dt := sharedDatatype("loaded-dates")
	bq := &fakeBQ{hiveErrs: map[string]error{part18: errFake}}
	storage := &fakeStorage{dirs: map[string][]gcs.Dir{"ndt7": {
		sharedDir("a", day18, time.Time{}), sharedDir("b", day18, time.Time{}), sharedDir("a", day19, time.Time{}),
	}}}
	c := NewClient(storage, bq)
	ds := bqfake.NewDataset(map[string]*bqfake.Table{}, nil, nil)

	if err := c.loadShared(context.Background(), ds, dt, periodOpts("daily")); err == nil {
		t.Error("Client.loadShared() error = nil, wantErr = true")
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

func TestClient_loadShared_LoadErrors(t *testing.T) {
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
			dt := sharedDatatype("load-errors-" + tt.name)
			bq := &fakeBQ{hiveErrs: map[string]error{part18: tt.err}}
			storage := &fakeStorage{dirs: map[string][]gcs.Dir{"ndt7": {sharedDir("a", day18, time.Time{})}}}
			c := NewClient(storage, bq)
			ds := bqfake.NewDataset(map[string]*bqfake.Table{}, nil, nil)

			if err := c.loadShared(context.Background(), ds, dt, periodOpts("daily")); err == nil {
				t.Error("Client.loadShared() error = nil, wantErr = true")
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

func TestClient_processDatatype_Shared(t *testing.T) {
	// The schema file is older than the table: only a shared-table datatype
	// has its schema updated anyway.
	old := time.Now().Add(-2 * time.Hour)
	newBQ := func() *fakeBQ {
		return &fakeBQ{
			datasets: map[string]*bqfake.Dataset{"dataset": bqfake.NewDataset(map[string]*bqfake.Table{}, nil, nil)},
			tables: map[string]*bigquery.TableMetadata{"datatype": {
				LastModifiedTime: time.Now().Add(-time.Hour),
			}},
		}
	}
	dirs := map[string][]gcs.Dir{"datatype": {
		{Path: "path-18", Date: day18},
		{Path: "path-19", Date: day19},
	}}

	tests := []struct {
		name       string
		hivePrefix string
		wantUpdate int
		wantLoad   int
		wantHive   int
	}{
		{name: "shared-schema-always-updated", hivePrefix: testPrefix, wantUpdate: 1, wantLoad: 0, wantHive: 2},
		{name: "v2-unchanged", hivePrefix: "", wantUpdate: 0, wantLoad: 2, wantHive: 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			bq := newBQ()
			dt := api.NewThirdPartyDatatype(api.DatatypeOpts{
				Name:        "datatype",
				Experiment:  "dataset",
				Version:     "v2",
				UpdatedTime: old,
			}, "")
			dt.HivePrefix = tt.hivePrefix
			c := NewClient(&fakeStorage{dirs: dirs}, bq)

			if err := c.processDatatype(context.Background(), dt, periodOpts("daily")); err != nil {
				t.Errorf("Client.processDatatype() error = %v, wantErr = false", err)
			}
			if bq.updateCount != tt.wantUpdate {
				t.Errorf("Client.processDatatype() update got = %d, want = %d", bq.updateCount, tt.wantUpdate)
			}
			if bq.loadCount != tt.wantLoad {
				t.Errorf("Client.processDatatype() load got = %d, want = %d", bq.loadCount, tt.wantLoad)
			}
			hive := 0
			for _, call := range bq.calls {
				if call.method == "LoadHive" {
					hive++
				}
			}
			if hive != tt.wantHive {
				t.Errorf("Client.processDatatype() hive load got = %d, want = %d", hive, tt.wantHive)
			}
		})
	}
}

func TestVersionLabel(t *testing.T) {
	// A v2 and a v3 datatype with the same experiment and name do not share a
	// series, and a datatype without a version counts as v1.
	const experiment = "version-label"
	for _, version := range []string{"", "v2", "v3"} {
		dt := api.NewMlabDatatype(api.DatatypeOpts{Name: "ndt7", Experiment: experiment, Version: version})
		if version == "v3" {
			dt.HivePrefix = testPrefix
		}
		storage := &fakeStorage{dirs: map[string][]gcs.Dir{"ndt7": {{Path: "path", Date: day18}}}}
		c := NewClient(storage, &fakeBQ{})
		if err := c.processDatatype(context.Background(), dt, periodOpts("daily")); err != nil {
			t.Fatalf("Client.processDatatype() error = %v, wantErr = false", err)
		}
	}

	for _, version := range []string{"v1", "v2", "v3"} {
		got := testutil.ToFloat64(metrics.BigQueryOperationsTotal.WithLabelValues(version, experiment, "ndt7", "load", "OK"))
		if got != 1 {
			t.Errorf("BigQueryOperationsTotal %s got = %v, want = 1", version, got)
		}
	}
}
