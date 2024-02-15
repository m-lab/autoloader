package handler

import (
	"net/url"
	"reflect"
	"testing"
	"time"

	"github.com/m-lab/go/timex"
)

func Test_getOpts(t *testing.T) {
	type args struct {
		values url.Values
	}
	tests := []struct {
		name    string
		values  url.Values
		want    *LoadOptions
		wantErr bool
	}{
		{
			name:    "success-range",
			values:  url.Values{"start": {"2023/01/01"}, "end": {"2023/03/29"}},
			want:    &LoadOptions{start: "2023/01/01", end: "2023/03/29", period: "custom"},
			wantErr: false,
		},
		{
			name:    "error-range",
			values:  url.Values{"start": {"2023-01-01"}, "end": {"2023-03-29"}},
			want:    nil,
			wantErr: true,
		},
		{
			name:    "success-period",
			values:  url.Values{"period": {"daily"}},
			want:    periodOpts("daily"),
			wantErr: false,
		},
		{
			name:    "error-period",
			values:  url.Values{"period": {"invalid-period"}},
			want:    nil,
			wantErr: true,
		},
		{
			name:    "missing",
			values:  url.Values{},
			want:    nil,
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := getOpts(tt.values)
			if (err != nil) != tt.wantErr {
				t.Errorf("getOpts() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("getOpts() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_periodOpts(t *testing.T) {
	now := time.Now().UTC()
	tests := []struct {
		name string
		p    string
		want *LoadOptions
	}{
		{
			name: "daily",
			p:    "daily",
			want: &LoadOptions{
				now.AddDate(0, 0, -1).Format(timex.YYYYMMDDWithSlash),
				now.AddDate(0, 0, 1).Format(timex.YYYYMMDDWithSlash),
				"daily",
			},
		},
		{
			name: "monthly",
			p:    "monthly",
			want: &LoadOptions{
				now.AddDate(0, -1, 0).Format(timex.YYYYMMDDWithSlash),
				now.AddDate(0, 0, -1).Format(timex.YYYYMMDDWithSlash),
				"monthly",
			},
		},
		{
			name: "annually",
			p:    "annually",
			want: &LoadOptions{
				now.AddDate(-1, 0, 0).Format(timex.YYYYMMDDWithSlash),
				now.AddDate(0, -1, 0).Format(timex.YYYYMMDDWithSlash),
				"annually",
			},
		},
		{
			name: "everything",
			p:    "everything",
			want: &LoadOptions{
				start,
				now.AddDate(0, 0, 1).Format(timex.YYYYMMDDWithSlash),
				"everything",
			},
		},
		{
			name: "invalid",
			p:    "invalid",
			want: nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := periodOpts(tt.p); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("periodOpts() = %v, want %v", got, tt.want)
			}
		})
	}
}
