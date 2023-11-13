package main

import (
	"context"
	"flag"
	"net/http"

	"cloud.google.com/go/bigquery"
	"cloud.google.com/go/storage"
	"github.com/m-lab/autoloader/bq"
	"github.com/m-lab/autoloader/gcs"
	gcsv2 "github.com/m-lab/autoloader/gcs/v2"
	"github.com/m-lab/autoloader/handler"
	"github.com/m-lab/go/flagx"
	"github.com/m-lab/go/prometheusx"
	"github.com/m-lab/go/rtx"
)

var (
	listenAddr          string
	bqProject           string
	viewProject         string
	gcsProject          string
	mlabBucket          string
	bucketNames         flagx.StringArray
	mainCtx, mainCancel = context.WithCancel(context.Background())
)

func init() {
	flag.StringVar(&listenAddr, "listenaddr", ":8080", "Address to listen on")
	flag.StringVar(&bqProject, "bq-project", "mlab-sandbox", "BigQuery project environment variable")
	flag.StringVar(&viewProject, "view-project", "mlab-sandbox", "BigQuery project for views")
	flag.StringVar(&gcsProject, "gcs-project", "mlab-sandbox", "GCS project")
	flag.StringVar(&mlabBucket, "mlab-bucket", "", "Archive bucket name containing data from M-Lab's platform")
	flag.Var(&bucketNames, "buckets", "Archive bucket names in Google Cloud Storage")
}

func main() {
	defer mainCancel()
	flag.Parse()
	rtx.Must(flagx.ArgsFromEnv(flag.CommandLine), "Could not parse env args")

	prom := prometheusx.MustServeMetrics()
	defer prom.Close()

	storage, err := storage.NewClient(mainCtx)
	rtx.Must(err, "Failed to create storage client")
	defer storage.Close()
	gcs := gcs.NewClient(storage, bucketNames, mlabBucket, gcsProject)

	bqMain, err := bigquery.NewClient(mainCtx, bqProject)
	rtx.Must(err, "Failed to create BigQuery client")
	defer bqMain.Close()

	bqView := bqMain
	if bqProject != viewProject {
		bqView, err = bigquery.NewClient(mainCtx, viewProject)
		rtx.Must(err, "Failed to create BigQuery view client")
		defer bqView.Close()
	}

	bq := bq.NewClient(bqMain, bqView)
	hndlr := handler.NewClient(gcs, bq)

	mux := http.NewServeMux()
	mux.HandleFunc("/v1/load", http.HandlerFunc(hndlr.Load))

	// V2 API.
	gcsV2 := gcsv2.NewClient(storage, bucketNames)
	hndlrV2 := handler.NewClient(gcsV2, bq)
	mux.HandleFunc("/v2/load", http.HandlerFunc(hndlrV2.Load))

	srv := &http.Server{
		Addr:    listenAddr,
		Handler: mux,
	}
	rtx.Must(srv.ListenAndServe(), "Could not start HTTP server")
	defer srv.Close()
}
