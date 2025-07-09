package v2

import (
	"strings"

	"github.com/m-lab/autoloader/api"
)

// NewMlabDatatype returns a new Datatype with M-Lab naming conventions.
func NewMlabDatatype(opts api.DatatypeOpts) *api.Datatype {
	return &api.Datatype{
		DatatypeOpts: opts,
		Namer:        NewNamer(opts, "mlab"),
		UpdateView:   true,
	}
}

// NewBYODatatype returns a new Datatype with BYOS/BYOD naming conventions.
func NewBYODatatype(opts api.DatatypeOpts, project string) *api.Datatype {
	sp := strings.TrimPrefix(project, "mlab-")
	return &api.Datatype{
		DatatypeOpts: opts,
		Namer:        NewNamer(opts, sp),
		UpdateView:   true,
	}
}
