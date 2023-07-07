package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"strings"

	"github.com/m-lab/go/flagx"
	"github.com/m-lab/go/storagex"

	"cloud.google.com/go/bigquery"
	"cloud.google.com/go/storage"
)

var (
	datatypes = flagx.StringArray{}
	project   string
	dryrun    bool
)

func init() {
	flag.StringVar(&project, "project", "mlab-sandbox", "Operate on the given project.")
	flag.BoolVar(&dryrun, "dryrun", true, "Take no action.")
	flag.Var(&datatypes, "datatype", "The experiment/datatype to delete from GCS and BQ.")
}

func main() {
	flag.Parse()
	flagx.ArgsFromEnv(flag.CommandLine)

	ctx := context.Background()
	sclient, err := storage.NewClient(ctx)
	if err != nil {
		panic(err)
	}
	defer sclient.Close()

	bqclient, err := bigquery.NewClient(ctx, project)
	if err != nil {
		panic(err)
	}
	defer bqclient.Close()

	if dryrun {
		log.Println("NOTE:")
		log.Println("NOTE: dryrun mode! Use -dryrun=false to delete data.")
		log.Println("NOTE:")
	}

	for _, dt := range datatypes {
		fields := strings.Split(dt, "/")
		if len(fields) != 2 {
			log.Printf("wrong datatype format; skipping %q", dt)
			continue
		}
		log.Printf("Removing: %s", dt)
		exp, datatype := fields[0], fields[1]
		b := storagex.NewBucket(sclient.Bucket("pusher-" + project))
		deleteObjects(ctx, b, fmt.Sprintf("autoload/v1/tables/%s/%s.table.json", exp, datatype))
		deleteObjects(ctx, b, fmt.Sprintf("autoload/v1/%s/%s", exp, datatype))

		b = storagex.NewBucket(sclient.Bucket("archive-" + project))
		deleteObjects(ctx, b, fmt.Sprintf("autoload/v1/tables/%s/%s.table.json", exp, datatype))
		deleteObjects(ctx, b, fmt.Sprintf("autoload/v1/%s/%s", exp, datatype))

		deleteTable(ctx, bqclient, "raw_"+exp, datatype)
	}

	if !dryrun {
		log.Println("WARNING:")
		log.Println("WARNING: active storage transfer jobs may recreate files just removed from the archive bucket")
		log.Println("WARNING:")
	}
}

func deleteObjects(ctx context.Context, bucket *storagex.Bucket, path string) error {
	attrs, err := bucket.Attrs(ctx)
	if err != nil {
		return err
	}
	log.Println("GCS", attrs.Name)
	return bucket.Walk(ctx, path, func(o *storagex.Object) error {
		log.Println("\tdelete:", o.ObjectName())
		if dryrun {
			return nil
		}
		return o.Delete(ctx)
	})
}
func deleteTable(ctx context.Context, client *bigquery.Client, dataset, table string) error {
	t := client.Dataset(dataset).Table(table)
	log.Println("BigQuery", client.Project())
	log.Println("\tdelete:", t.DatasetID, t.TableID)
	if dryrun {
		return nil
	}
	return t.Delete(ctx)
}
