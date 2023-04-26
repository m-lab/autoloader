package api

import "testing"

func TestMlabNamer_Dataset(t *testing.T) {
	tests := []struct {
		name       string
		datatype   string
		experiment string
		want       string
	}{
		{
			name:       "sample",
			datatype:   "datatype",
			experiment: "experiment",
			want:       "raw_experiment",
		},
		{
			name:       "ndt",
			datatype:   "ndt7",
			experiment: "ndt",
			want:       "raw_ndt",
		},
		{
			name:       "host",
			datatype:   "nodeinfo1",
			experiment: "host",
			want:       "raw_host",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := NewMlabNamer(tt.datatype, tt.experiment)
			if got := m.Dataset(); got != tt.want {
				t.Errorf("MlabNamer.Dataset() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestMlabNamer_Table(t *testing.T) {
	tests := []struct {
		name       string
		datatype   string
		experiment string
		want       string
	}{
		{
			name:       "sample",
			datatype:   "datatype",
			experiment: "experiment",
			want:       "datatype",
		},
		{
			name:       "ndt",
			datatype:   "ndt7",
			experiment: "ndt",
			want:       "ndt7",
		},
		{
			name:       "host",
			datatype:   "nodeinfo1",
			experiment: "host",
			want:       "nodeinfo1",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := NewMlabNamer(tt.datatype, tt.experiment)
			if got := m.Table(); got != tt.want {
				t.Errorf("MlabNamer.Table() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestMlabNamer_ViewDataset(t *testing.T) {
	tests := []struct {
		name       string
		datatype   string
		experiment string
		want       string
	}{
		{
			name:       "sample",
			datatype:   "datatype",
			experiment: "experiment",
			want:       "experiment_raw",
		},
		{
			name:       "ndt",
			datatype:   "ndt7",
			experiment: "ndt",
			want:       "ndt_raw",
		},
		{
			name:       "host",
			datatype:   "nodeinfo1",
			experiment: "host",
			want:       "host_raw",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := NewMlabNamer(tt.datatype, tt.experiment)
			if got := m.ViewDataset(); got != tt.want {
				t.Errorf("MlabNamer.ViewDataset() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestMlabNamer_ViewTable(t *testing.T) {
	tests := []struct {
		name       string
		datatype   string
		experiment string
		want       string
	}{
		{
			name:       "sample",
			datatype:   "datatype",
			experiment: "experiment",
			want:       "datatype",
		},
		{
			name:       "ndt",
			datatype:   "ndt7",
			experiment: "ndt",
			want:       "ndt7",
		},
		{
			name:       "host",
			datatype:   "nodeinfo1",
			experiment: "host",
			want:       "nodeinfo1",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := NewMlabNamer(tt.datatype, tt.experiment)
			if got := m.ViewTable(); got != tt.want {
				t.Errorf("MlabNamer.ViewTable() = %v, want %v", got, tt.want)
			}
		})
	}
}
