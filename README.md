# Autoloader
Autoloads Measurement Results into BigQuery.

## Layouts

The autoloader serves one route per archive layout: `/v1/load`, `/v2/load` and `/v3/load`. A datatype exists because a schema file exists under `autoload/<version>/tables/`. Schema files are uploaded by hand.

### v3: one table per datatype for all organizations

- Objects: `autoload/v3/org=<name>/<experiment>/<datatype>/YYYY/MM/DD/<file>.jsonl.gz`.
- Schema files: `autoload/v3/tables/<experiment>/<datatype>.table.json`. The schema must not define an `org` column; the autoloader appends it.
- Destination: dataset `autoload_v3_<experiment>`, table `<datatype>_raw`, partitioned by day on `date` and clustered by `org`. The `org` column is taken from the object path, not from the rows.
- No two buckets in one deployment may hold the same experiment and datatype under `autoload/v3/tables/`, because the destination name does not include the bucket.

Loading:

- Each date is loaded with one job that names every organization's directory and replaces that day's partition.
- A date is loaded only when its partition is empty or an object for that date is newer than the partition's last load (less a margin of 6 hours). A request with explicit `start` and `end` dates always loads, which is how to force a reload, for example after removing an object.
- If the job for a date fails, for any reason, the day's partition is left as it was: no organization gets new data for that date until the cause is gone. There is no retry within a run; the date is tried again on every later run that covers it, and other dates are not affected.
- Each failed job is counted in `autoloader_load_errors_total{version, experiment, datatype, org, reason}`. `org` is the organization whose file made the job fail, taken from the file that BigQuery's error names, and `reason` is BigQuery's reason (for example `invalid` for bad data, `backendError` for a transient failure). `org` is `unknown` when the error names no file, which is the case for backend errors and for a row whose `date` differs from its directory's date.
