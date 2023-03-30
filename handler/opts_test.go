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
			want:    &LoadOptions{start: "2023/01/01", end: "2023/03/29"},
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
			values:  url.Values{"period": {"day"}},
			want:    periodOpts("day"),
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
			name: "day",
			p:    "day",
			want: &LoadOptions{
				now.AddDate(0, 0, -1).Format(timex.YYYYMMDDWithSlash),
				now.AddDate(0, 0, 1).Format(timex.YYYYMMDDWithSlash),
			},
		},
		{
			name: "month",
			p:    "month",
			want: &LoadOptions{
				now.AddDate(0, -1, 0).Format(timex.YYYYMMDDWithSlash),
				now.AddDate(0, 0, -1).Format(timex.YYYYMMDDWithSlash),
			},
		},
		{
			name: "all",
			p:    "all",
			want: &LoadOptions{
				start,
				now.AddDate(0, -1, 0).Format(timex.YYYYMMDDWithSlash),
			},
		},
		{
			name: "new",
			p:    "new",
			want: &LoadOptions{
				start,
				now.AddDate(0, 0, 1).Format(timex.YYYYMMDDWithSlash),
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
