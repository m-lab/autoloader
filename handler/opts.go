package handler

import (
	"errors"
	"net/url"
	"time"

	"github.com/m-lab/go/timex"
)

// LoadOptions allows clients to specify parameters modifying how
// the data is loaded.
type LoadOptions struct {
	start string
	end   string
}

const (
	start = "0000/00/00"
)

var (
	errDate   = errors.New("invalid date format (want YYYY/MM/DD)")
	errPeriod = errors.New("invalid or missing period (want 'day', 'month', 'all', or 'new'")
)

func getOpts(values url.Values) (*LoadOptions, error) {
	s := values.Get("start")
	e := values.Get("end")

	// Specific start to end range provided.
	if s != "" && e != "" {
		_, startErr := time.Parse(timex.YYYYMMDDWithSlash, s)
		_, endErr := time.Parse(timex.YYYYMMDDWithSlash, e)
		if startErr != nil || endErr != nil {
			return nil, errDate
		}
		return &LoadOptions{s, e}, nil
	}

	// Time period provided.
	period := values.Get("period")
	opts := periodOpts(period)
	if opts != nil {
		return opts, nil
	}
	return nil, errPeriod
}

func periodOpts(p string) *LoadOptions {
	now := time.Now().UTC()
	tomorrow := now.AddDate(0, 0, 1).Format(timex.YYYYMMDDWithSlash)
	yesterday := now.AddDate(0, 0, -1).Format(timex.YYYYMMDDWithSlash)
	month := now.AddDate(0, -1, 0).Format(timex.YYYYMMDDWithSlash)

	switch p {
	case "day":
		return &LoadOptions{yesterday, tomorrow}
	case "month":
		return &LoadOptions{month, yesterday}
	case "all":
		return &LoadOptions{start, month}
	case "new":
		return &LoadOptions{start, tomorrow}
	}

	return nil
}
