package handler

import (
	"context"
	"errors"
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
	// The calls of a fallback for a and b on the 18th, up to the copy.
	fallback18 := []call{
		{method: "LoadHive", prefix: testPrefix, table: part18, uris: []string{path("a", day18), path("b", day18)}},
		{method: "EnsureStagingDataset"},
		{method: "EnsureStagingTable", table: "a"},
		{method: "LoadHive", prefix: testPrefix, table: "ndt7_a$20261118", uris: []string{path("a", day18)}},
		{method: "EnsureStagingTable", table: "b"},
	}
	loadB18 := call{method: "LoadHive", prefix: testPrefix, table: "ndt7_b$20261118", uris: []string{path("b", day18)}}
	copyA18 := call{method: "CopyPartitions", table: part18, srcs: []string{"ndt7_a$20261118"}}

	tests := []struct {
		name         string
		dirs         []gcs.Dir
		bq           *fakeBQ
		opts         *LoadOptions
		want         []call
		wantExcluded map[string]float64
		wantErr      bool
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
			name: "shared-job-fails",
			dirs: []gcs.Dir{sharedDir("a", day18, T), sharedDir("b", day18, T)},
			bq:   &fakeBQ{hiveErrs: map[string]error{part18: errFake}},
			want: append(append([]call{}, fallback18...), loadB18,
				call{method: "CopyPartitions", table: part18, srcs: []string{"ndt7_a$20261118", "ndt7_b$20261118"}}),
		},
		{
			name:         "one-org-bad",
			dirs:         []gcs.Dir{sharedDir("a", day18, T), sharedDir("b", day18, T)},
			bq:           &fakeBQ{hiveErrs: map[string]error{part18: errFake, "ndt7_b$20261118": errFake}},
			want:         append(append([]call{}, fallback18...), loadB18, copyA18),
			wantExcluded: map[string]float64{"b": 1},
			wantErr:      true,
		},
		{
			name: "staging-table-fails",
			dirs: []gcs.Dir{sharedDir("a", day18, T), sharedDir("b", day18, T)},
			bq: &fakeBQ{
				hiveErrs:      map[string]error{part18: errFake},
				stagingTblErr: map[string]error{"b": errFake},
			},
			want:         append(append([]call{}, fallback18...), copyA18),
			wantExcluded: map[string]float64{"b": 1},
			wantErr:      true,
		},
		{
			name: "staging-dataset-fails",
			dirs: []gcs.Dir{sharedDir("a", day18, T), sharedDir("b", day18, T)},
			bq:   &fakeBQ{hiveErrs: map[string]error{part18: errFake}, stagingDsErr: errFake},
			want: []call{
				{method: "LoadHive", prefix: testPrefix, table: part18, uris: []string{path("a", day18), path("b", day18)}},
				{method: "EnsureStagingDataset"},
			},
			wantErr: true,
		},
		{
			name: "a-later-date-still-loads",
			dirs: []gcs.Dir{sharedDir("a", day18, T), sharedDir("b", day18, T), sharedDir("a", day19, T)},
			bq:   &fakeBQ{hiveErrs: map[string]error{part18: errFake, "ndt7_b$20261118": errFake}},
			want: append(append([]call{}, fallback18...), loadB18, copyA18,
				call{method: "LoadHive", prefix: testPrefix, table: part19, uris: []string{path("a", day19)}}),
			wantExcluded: map[string]float64{"b": 1},
			wantErr:      true,
		},
		{
			name: "all-orgs-bad",
			dirs: []gcs.Dir{sharedDir("a", day18, T), sharedDir("b", day18, T)},
			bq: &fakeBQ{hiveErrs: map[string]error{
				part18: errFake, "ndt7_a$20261118": errFake, "ndt7_b$20261118": errFake,
			}},
			// Nothing was copied, so no org counts as excluded.
			want:    append(append([]call{}, fallback18...), loadB18),
			wantErr: true,
		},
		{
			// The partition is untouched, so b does not count as excluded.
			name: "copy-fails",
			dirs: []gcs.Dir{sharedDir("a", day18, T), sharedDir("b", day18, T)},
			bq: &fakeBQ{
				hiveErrs: map[string]error{part18: errFake, "ndt7_b$20261118": errFake},
				copyErr:  errFake,
			},
			want:    append(append([]call{}, fallback18...), loadB18, copyA18),
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

			for _, org := range []string{"a", "b"} {
				got := testutil.ToFloat64(metrics.ExcludedOrgsTotal.WithLabelValues("v3", tt.name, "ndt7", org))
				if got != tt.wantExcluded[org] {
					t.Errorf("Client.loadShared() excluded org %s got = %v, want = %v", org, got, tt.wantExcluded[org])
				}
			}
		})
	}
}

func TestClient_loadShared_ReturnsLastError(t *testing.T) {
	// The 18th fails in the copy and the 19th loads cleanly: the 19th is still
	// loaded and the error of the 18th is returned.
	bq := &fakeBQ{hiveErrs: map[string]error{part18: errors.New("shared")}, copyErr: errFake}
	storage := &fakeStorage{dirs: map[string][]gcs.Dir{"ndt7": {
		sharedDir("a", day18, time.Time{}), sharedDir("a", day19, time.Time{}),
	}}}
	c := NewClient(storage, bq)
	ds := bqfake.NewDataset(map[string]*bqfake.Table{}, nil, nil)

	err := c.loadShared(context.Background(), ds, sharedDatatype("last-error"), periodOpts("daily"))
	if !errors.Is(err, errFake) {
		t.Errorf("Client.loadShared() error = %v, want = %v", err, errFake)
	}
	last := bq.calls[len(bq.calls)-1]
	if last.method != "LoadHive" || last.table != part19 {
		t.Errorf("Client.loadShared() last call = %v, want a load of %s", last, part19)
	}
}

func TestClient_loadShared_Cancelled(t *testing.T) {
	// The request is cancelled during the fallback of the 18th: no org counts
	// as excluded, nothing is copied and the 19th is not attempted.
	ctx, cancel := context.WithCancel(context.Background())
	bq := &cancellingBQ{fakeBQ: &fakeBQ{hiveErrs: map[string]error{part18: errFake}}, cancel: cancel}
	storage := &fakeStorage{dirs: map[string][]gcs.Dir{"ndt7": {
		sharedDir("a", day18, time.Time{}), sharedDir("b", day18, time.Time{}), sharedDir("a", day19, time.Time{}),
	}}}
	c := NewClient(storage, bq)
	ds := bqfake.NewDataset(map[string]*bqfake.Table{}, nil, nil)

	err := c.loadShared(ctx, ds, sharedDatatype("cancelled"), periodOpts("daily"))
	if !errors.Is(err, context.Canceled) {
		t.Errorf("Client.loadShared() error = %v, want = %v", err, context.Canceled)
	}
	for _, call := range bq.calls {
		if call.method == "CopyPartitions" || call.table == part19 {
			t.Errorf("Client.loadShared() unexpected call after cancellation: %v", call)
		}
	}
	for _, org := range []string{"a", "b"} {
		got := testutil.ToFloat64(metrics.ExcludedOrgsTotal.WithLabelValues("v3", "cancelled", "ndt7", org))
		if got != 0 {
			t.Errorf("Client.loadShared() excluded org %s got = %v, want = 0", org, got)
		}
	}
}

// cancellingBQ cancels the request during the first staging load, which then
// fails like every call made with a cancelled context.
type cancellingBQ struct {
	*fakeBQ
	cancel context.CancelFunc
}

func (bq *cancellingBQ) LoadHive(ctx context.Context, ds bqiface.Dataset, name string, dt *api.Datatype, uri ...string) error {
	err := bq.fakeBQ.LoadHive(ctx, ds, name, dt, uri...)
	if name == "ndt7_a$20261118" {
		bq.cancel()
		return context.Canceled
	}
	return err
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
	// The 18th loads with an org excluded (error), the 19th loads cleanly (OK).
	dt := sharedDatatype("loaded-dates")
	bq := &fakeBQ{hiveErrs: map[string]error{part18: errFake, "ndt7_b$20261118": errFake}}
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
