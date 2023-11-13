package v2

import (
	"context"
	"fmt"
	"path"
	"strings"

	"cloud.google.com/go/storage"
)

// SchemaPath interprets the syntax of a datatype's schema path in GCS.
type SchemaPath struct {
	Datatype      string   // Datatype name.
	Experiment    string   // Experiment name.
	Organizations []string // Organizations uploading data for the datatype.
}

// NewSchemaPath returns a new SchemaPath object.
func NewSchemaPath(ctx context.Context, b *BucketV2, schemaPath string) (*SchemaPath, error) {
	parts := strings.Split(schemaPath, "/")

	switch len(parts) {
	case 6:
		// In-band schema path "autoload/v2/tables/<organization>/<experiment>/<datatype>.table.json".
		return &SchemaPath{
			Datatype:      strings.TrimSuffix(parts[5], schemaFileSuffix),
			Experiment:    parts[4],
			Organizations: []string{parts[3]},
		}, nil
	case 5:
		// Out-of-band schema path "autoload/v2/tables/<experiment>/<datatype>.table.json".
		p := &SchemaPath{
			Datatype:   strings.TrimSuffix(parts[4], schemaFileSuffix),
			Experiment: parts[3],
		}
		p.Organizations = datatypeOrgs(ctx, b, p.Experiment, p.Datatype)
		return p, nil
	default:
		return nil, fmt.Errorf("invalid GCS path %s", schemaPath)
	}
}

func datatypeOrgs(ctx context.Context, b *BucketV2, exp, dt string) []string {
	orgs := make([]string, 0)

	for _, org := range b.Organizations {
		// Get iterator for "autoload/v2/<organization>/<experiment>/<datatype>/".
		p := path.Join(prefix, org, exp, dt) + "/"
		it := b.Objects(ctx, &storage.Query{
			Prefix:    p,
			Delimiter: "/",
		})

		// Check if path exists.
		_, err := it.Next()
		if err != nil {
			continue
		}
		orgs = append(orgs, org)
	}

	return orgs
}
