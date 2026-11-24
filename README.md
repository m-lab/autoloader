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
- If the job for a date fails, the date is loaded organization by organization into staging tables (dataset `autoload_v3_<experiment>_staging`, tables expire after 48 hours), and the day's partition is replaced with a copy of the organizations that loaded.
- An organization that cannot be loaded is excluded: it has no rows for that datatype and date afterwards, including rows that an earlier run had loaded. The date is reported as an error and counted in `autoloader_excluded_orgs_total`. Other organizations are not affected. Once the cause is fixed, reload the date with explicit `start` and `end` dates.
