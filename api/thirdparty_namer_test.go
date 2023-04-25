package api

import "testing"

func TestThirdPartyNamer_Dataset(t *testing.T) {
	tests := []struct {
		name       string
		datatype   string
		experiment string
		project    string
		want       string
	}{
		{
			name:       "sample",
			datatype:   "datatype",
			experiment: "experiment",
			project:    "project",
			want:       "experiment",
		},
		{
			name:       "cloudflare",
			datatype:   "speed1",
			experiment: "speedtest",
			project:    "mlab-cloudflare",
			want:       "speedtest",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tp := NewThirdPartyNamer(tt.datatype, tt.experiment, tt.project)
			if got := tp.Dataset(); got != tt.want {
				t.Errorf("ThirdPartyNamer.Dataset() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestThirdPartyNamer_Table(t *testing.T) {
	tests := []struct {
		name       string
		datatype   string
		experiment string
		project    string
		want       string
	}{
		{
			name:       "sample",
			datatype:   "datatype",
			experiment: "experiment",
			project:    "project",
			want:       "datatype",
		},
		{
			name:       "cloudflare",
			datatype:   "speed1",
			experiment: "speedtest",
			project:    "mlab-cloudflare",
			want:       "speed1",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tp := NewThirdPartyNamer(tt.datatype, tt.experiment, tt.project)
			if got := tp.Table(); got != tt.want {
				t.Errorf("ThirdPartyNamer.Table() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestThirdPartyNamer_ViewDataset(t *testing.T) {
	tests := []struct {
		name       string
		datatype   string
		experiment string
		project    string
		want       string
	}{
		{
			name:       "sample",
			datatype:   "datatype",
			experiment: "experiment",
			project:    "project",
			want:       "project",
		},
		{
			name:       "cloudflare",
			datatype:   "speed1",
			experiment: "speedtest",
			project:    "mlab-cloudflare",
			want:       "cloudflare",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tp := NewThirdPartyNamer(tt.datatype, tt.experiment, tt.project)
			if got := tp.ViewDataset(); got != tt.want {
				t.Errorf("ThirdPartyNamer.ViewDataset() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestThirdPartyNamer_ViewTable(t *testing.T) {
	tests := []struct {
		name       string
		datatype   string
		experiment string
		project    string
		want       string
	}{
		{
			name:       "sample",
			datatype:   "datatype",
			experiment: "experiment",
			project:    "project",
			want:       "experiment_datatype",
		},
		{
			name:       "cloudflare",
			datatype:   "speed1",
			experiment: "speedtest",
			project:    "mlab-cloudflare",
			want:       "speedtest_speed1",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tp := NewThirdPartyNamer(tt.datatype, tt.experiment, tt.project)
			if got := tp.ViewTable(); got != tt.want {
				t.Errorf("ThirdPartyNamer.ViewTable() = %v, want %v", got, tt.want)
			}
		})
	}
}
