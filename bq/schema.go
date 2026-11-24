package bq

import (
	"errors"
	"fmt"
	"strings"

	"cloud.google.com/go/bigquery"
	"github.com/m-lab/autoloader/api"
)

// orgColumn is the column of a shared table that holds the organization,
// taken from the "org=<name>" component of the object path.
const orgColumn = "org"

// ErrOrgColumn is returned when the schema file of a shared-table datatype
// defines the column that the autoloader adds itself.
var ErrOrgColumn = errors.New("schema must not define an org column")

// tableSchema returns the BigQuery schema of the table for the input datatype.
// For a datatype with a hive prefix, it appends a last nullable STRING column
// named "org". In that case a schema file that already has a top-level "org"
// column, in any letter case, is an error wrapping ErrOrgColumn.
func tableSchema(dt *api.Datatype) (bigquery.Schema, error) {
	schema, err := bigquery.SchemaFromJSON(dt.Schema)
	if err != nil {
		return nil, err
	}
	if dt.HivePrefix == "" {
		return schema, nil
	}
	for _, f := range schema {
		// BigQuery column names are case-insensitive.
		if strings.EqualFold(f.Name, orgColumn) {
			return nil, fmt.Errorf("%w: found %q", ErrOrgColumn, f.Name)
		}
	}
	return append(schema, &bigquery.FieldSchema{
		Name: orgColumn,
		Type: bigquery.StringFieldType,
	}), nil
}

// tableMetadata returns the metadata used to create a table with the input
// name for the input datatype: the schema from tableSchema, daily partitioning
// on the "date" column with a required partition filter and, for a datatype
// with a hive prefix, clustering by the "org" column.
func tableMetadata(dt *api.Datatype, name string) (*bigquery.TableMetadata, error) {
	schema, err := tableSchema(dt)
	if err != nil {
		return nil, err
	}
	md := &bigquery.TableMetadata{
		Name:   name,
		Schema: schema,
		TimePartitioning: &bigquery.TimePartitioning{
			Type:                   bigquery.DayPartitioningType,
			Field:                  "date",
			RequirePartitionFilter: true,
		},
	}
	if dt.HivePrefix != "" {
		md.Clustering = &bigquery.Clustering{Fields: []string{orgColumn}}
	}
	return md, nil
}
