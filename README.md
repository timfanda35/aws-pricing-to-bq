# aws-pricing-to-bq

Daily loader for the [AWS Price List Bulk API](https://docs.aws.amazon.com/awsaccountbilling/latest/aboutv2/using-price-list-api.html), sinking to **BigQuery** via GCS. Runs as a Cloud Run Job; consumers query the dataset directly via SQL.

Sibling of [`azure-pricing-to-bq`](../azure-pricing-to-bq) and the (Postgres-targeted) [`aws-pricing-list-loader`](https://github.com/timfanda35/aws-pricing-list-loader) — same shape, same query mental model, just different cloud source.

## What lands in BigQuery

| Table | Shape | Who reads it |
|---|---|---|
| `aws_pricing` | Latest snapshot. Not partitioned. Clustered by `service_code, region_code, sku`. | **Default for consumers.** Plain `SELECT *` returns today's prices. |
| `aws_pricing_history` | Append-only history. `PARTITION BY ingestion_date`. **`require_partition_filter = TRUE`.** Same clustering. | Time-travel queries (price changes over time). |
| `aws_pricing_versions` | One row per (service, region, offer_type) — the AWS-supplied `version` we last loaded. | Internal: powers the incremental diff so the loader skips unchanged offers. |
| `pricing_runs` | Audit: one row per loader invocation. | Operations / monitoring. |

The live `aws_pricing` table is rebuilt at the end of every successful run by a single atomic `CREATE OR REPLACE TABLE` — consumers either see yesterday's snapshot or today's, never a half-loaded mix.

## How incremental loading works (the 20 GB problem)

AWS publishes pricing as ~6000 `(service, region)` JSON files, ~5–10 GB total. **AWS does not publish deltas** — each file is monolithic. But each file carries a `version` timestamp embedded in its URL, and most files don't change on any given day.

So every run:

1. Walks the [master index](https://pricing.us-east-1.amazonaws.com/offers/v1.0/aws/index.json) and every per-service `region_index.json`.
2. Joins the discovered `(service, region, version)` tuples against `aws_pricing_versions`.
3. Downloads only the rows whose version changed.
4. LOADs those into today's history partition.
5. Carries forward the unchanged `(service, region)` rows from the previous partition with a single `INSERT ... SELECT * REPLACE (ingestion_date)`.
6. Rebuilds the live `aws_pricing` table from today's partition.

A typical day moves a few hundred MB at most.

## Schema strategy: one table, JSON for the variable parts

AWS pricing data is notoriously irregular — `products[*].attributes` varies per service (EC2 has `instanceType/vcpu/memory`; S3 has `storageClass`; RDS has `engineCode`; ...) and AWS adds new keys without warning.

Rather than 200+ per-service tables, this project lands **one unified `aws_pricing_history` table** with stable columns promoted to typed columns and the service-specific bag stored in a native BigQuery `JSON` column:

```sql
SELECT
  region_code,
  JSON_VALUE(attributes, '$.instanceType') AS instance_type,
  JSON_VALUE(attributes, '$.vcpu')         AS vcpu,
  price_per_unit
FROM `<project>.aws_pricing.aws_pricing`
WHERE service_code = 'AmazonEC2'
  AND term_type    = 'OnDemand'
  AND region_code  = 'us-east-1'
LIMIT 20;
```

New AWS attributes just appear inside the JSON. Zero DDL evolution.

**Price columns use `BIGNUMERIC`, not `NUMERIC`.** `NUMERIC` only carries 9 fractional digits, but AWS routinely publishes prices like `0.0001000000` (10 digits) and `1.0E-4` — `NUMERIC` would silently round those.

## Local dev

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements-dev.txt
cp .env.example .env   # fill GCP_PROJECT and GCS_STAGING_BUCKET

ruff check .
pytest -q
```

### Filtered smoke against a real GCP project

```bash
gcloud auth application-default login
export GCP_PROJECT=<dev-project>
export BQ_DATASET=aws_pricing_dev
export GCS_STAGING_BUCKET=<dev-project>-aws-pricing-staging
# MVP: validate end-to-end with just a few services
export AWS_SERVICE_FILTER="AmazonEC2,AmazonRDS,AmazonS3"
export AWS_INCLUDE_SAVINGS_PLANS=false

python -m aws_pricing_to_bq setup
python -m aws_pricing_to_bq load
python -m aws_pricing_to_bq runs --limit 5

bq query --use_legacy_sql=false \
  'SELECT service_code, region_code, COUNT(*) AS rows
   FROM `'"$GCP_PROJECT"'.aws_pricing_dev.aws_pricing`
   GROUP BY 1, 2 ORDER BY rows DESC LIMIT 10'
```

A second run shortly after should be a near-noop: `changed=0`, `skipped≈300+`, elapsed under a minute. That's the version-diff doing its job.

The history table will reject any query without a `WHERE ingestion_date = …` filter — that protects all consumers from accidental full-table scans.

## CLI

```bash
python -m aws_pricing_to_bq setup
python -m aws_pricing_to_bq load [--force] [--service-filter "AmazonEC2,AmazonRDS"]
python -m aws_pricing_to_bq runs [--limit N]
```

`--force` bypasses the version diff and re-downloads every `(service, region)`. Useful for backfills or recovering from a corrupted partition.

## Docker / Cloud Run Job

```bash
docker build -t aws-pricing-to-bq:dev .

# Local smoke (uses your gcloud ADC creds):
docker compose up
```

The image's default `CMD` is `python run_job.py`, which is exactly what Cloud Run Job invokes.

## Deployment (GCP)

1. **GitHub Actions → GHCR** — on every push to `main` (or a `v*` tag), the workflow in `.github/workflows/docker-publish.yml` runs `pytest` and, on success, builds and pushes the image to `ghcr.io/<owner>/<repo>`:
   - `latest` — tracks the `main` branch
   - `<semver>` — created when you push a `v*` tag (e.g. `v1.2.3`)

2. **GCS staging bucket** — same region as the BQ dataset. **Add a lifecycle rule to delete objects older than 7 days** so failed-run debris cleans itself up.
3. **Service account** for the Cloud Run Job:
   - `roles/bigquery.dataEditor` on the dataset
   - `roles/bigquery.jobUser` on the project
   - `roles/storage.objectAdmin` on the staging bucket
4. **Cloud Run Job** `aws-pricing-loader-job`:
   - **task timeout 3600s** (default 600s is too short for a full first load)
   - parallelism 1, max retries 1
   - Image: `ghcr.io/<owner>/<repo>:latest` (or pin a semver tag)
   - `CMD ["python","run_job.py"]`
5. **Cloud Scheduler**: daily 02:00 UTC → Cloud Run Job admin API with OIDC token (scheduler SA needs `roles/run.invoker`).

There is no Cloud Run Service deployment.

## Sample consumer queries

EC2 m5.large OnDemand by region:

```sql
SELECT
  region_code,
  JSON_VALUE(attributes, '$.instanceType') AS instance_type,
  price_per_unit,
  unit
FROM `<project>.aws_pricing.aws_pricing`
WHERE service_code = 'AmazonEC2'
  AND term_type    = 'OnDemand'
  AND JSON_VALUE(attributes, '$.instanceType') = 'm5.large'
  AND JSON_VALUE(attributes, '$.operatingSystem') = 'Linux'
  AND JSON_VALUE(attributes, '$.tenancy') = 'Shared'
  AND JSON_VALUE(attributes, '$.preInstalledSw') = 'NA'
ORDER BY region_code;
```

S3 storage classes pricing:

```sql
SELECT
  region_code,
  JSON_VALUE(attributes, '$.storageClass') AS storage_class,
  price_per_unit,
  unit
FROM `<project>.aws_pricing.aws_pricing`
WHERE service_code = 'AmazonS3'
  AND term_type    = 'OnDemand'
ORDER BY region_code, storage_class;
```

Price-over-time (history table; partition filter required):

```sql
SELECT
  ingestion_date,
  region_code,
  price_per_unit
FROM `<project>.aws_pricing.aws_pricing_history`
WHERE ingestion_date BETWEEN DATE '2026-05-01' AND DATE '2026-05-27'
  AND service_code = 'AmazonEC2'
  AND JSON_VALUE(attributes, '$.instanceType') = 'm5.large'
  AND region_code = 'us-east-1'
  AND term_type = 'OnDemand'
ORDER BY ingestion_date;
```

## Cross-team access

Granting another team read access:

- `roles/bigquery.dataViewer` on the dataset (or per-table on `aws_pricing` only for tighter scope).
- They also need `roles/bigquery.jobUser` in **their own project** to run queries — they pay their own query cost (standard BigQuery billing pattern).

## Configuration

| Var | Default | Purpose |
|---|---|---|
| `GCP_PROJECT` | — | GCP project ID (required) |
| `BQ_DATASET` | `aws_pricing` | dataset name |
| `BQ_LOCATION` | `US` | dataset region; must match staging bucket region |
| `GCS_STAGING_BUCKET` | — | bucket for intermediate JSONL files (required) |
| `GCS_STAGING_PREFIX` | `ingestion/` | object key prefix |
| `AWS_PRICING_BASE_URL` | `https://pricing.us-east-1.amazonaws.com` | |
| `AWS_REQUEST_TIMEOUT_S` | `60` | per-request timeout (full EC2 offer ~200 MB) |
| `AWS_MAX_RETRIES` | `5` | tenacity retries on 429 / 5xx |
| `AWS_MAX_WORKERS` | `10` | parallel offer downloads |
| `AWS_SERVICE_FILTER` | `` | comma-sep offer codes, e.g. `AmazonEC2,AmazonRDS,AmazonS3`. Empty = all. |
| `AWS_INCLUDE_SAVINGS_PLANS` | `true` | also load Compute / Database / ML Savings Plans |
| `AWS_INCLUDE_RESERVED` | `true` | also flatten Reserved Instance terms |
| `HTTP_PROXY` | `` | proxy for outbound HTTP |
| `HTTPS_PROXY` | `` | proxy for outbound HTTPS |
| `NO_PROXY` | `` | proxy bypass list |
| `JSONL_BATCH_SIZE` | `10000` | items per uploaded JSONL file |
| `LOG_LEVEL` | `INFO` | |

## Memory budget on Cloud Run

Cloud Run's `/tmp` is **tmpfs (RAM-backed)** — anything we write to disk counts
against the instance's memory limit. The loader compensates by gzipping the
downloaded offer JSON on tmpfs (transparently re-opened by the parser via
magic-byte detection), which cuts that 6-8x. The dominant per-worker costs
during an offer download:

| Per worker (large offer in flight) | Cost |
|---|---|
| Downloaded offer JSON on `/tmp`, gzipped (EC2 us-east-1) | ~30 MB |
| `products` lookup dict in Python (50K-100K SKUs × full attributes) | ~300 MB |
| In-flight NDJSON temp file on `/tmp` | ~50 MB |
| **Subtotal per worker** | **~380 MB** |

With `AWS_MAX_WORKERS=3` and Python runtime + libraries (~300 MB), total peak
is ~1.5 GB — fits comfortably in a 4 GiB instance. Sizing reference:

| Cloud Run memory | Recommended `AWS_MAX_WORKERS` |
|---|---|
| 4 GiB | 3 (default) |
| 8 GiB | 5–6 |
| 16 GiB | 8–10 |

`AWS_DISCOVER_WORKERS` controls concurrent `region_index.json` fetches and
those responses are tiny — leave it at 10 regardless of instance size; it
doesn't affect memory.

OOM symptom in Cloud Logging looks like: container killed exit code 137,
with the last `mem.snapshot` log showing peak_rss climbing past the limit
during the parallel-download phase. If that happens, halve `AWS_MAX_WORKERS`.

## Observability

- **Container-level memory**: `run.googleapis.com/container/memory/utilizations` in Cloud Monitoring — the source of truth for "did this job OOM". Cloud Run publishes this automatically, no instrumentation needed.
- **Per-stage RSS in logs**: every run emits structured `mem.snapshot` log lines at start, after discovery, after the parallel download pool, after the LOAD JOB, after the live-table swap, and per `(service, region)` target. Useful when you need to figure out *which stage* of a load drove the peak — the container metric tells you peak but not why.
  - Sample log line: `mem.snapshot label=download.done rss_mb=412.3 peak_rss_mb=415.7 service=AmazonEC2 region=us-east-1 rows=1234567`
  - Grep `mem.snapshot` in Cloud Logging Explorer to see the full profile of a run.
- **Runtime banner**: first log line of every job records Python version and the active `ijson.backend`. If a future image change loses the C wheel and falls back to the pure-Python parser (3-10x slower on EC2 us-east-1), you'll see it at a glance.

## Design notes

- **Per-(service, region) version diff**: the heart of the incremental story. The full AWS dataset is ~5–10 GB but most files are unchanged on any given day.
- **JSON column for service-specific attributes**: AWS publishes incompatible attribute keys per service. A unified table with `attributes JSON` keeps the query interface uniform (`JSON_VALUE(...)`) while absorbing all variability.
- **BIGNUMERIC for prices**: AWS routinely publishes 10+ decimal places. `NUMERIC` would round them.
- **Partition decorator + WRITE_TRUNCATE + carryforward INSERT**: today's partition is composed of (a) fresh rows from changed `(service, region)` files via LOAD JOB and (b) carried-forward rows from yesterday's partition for unchanged pairs.
- **`CREATE OR REPLACE TABLE` for the live table**: single atomic statement; no rename gymnastics.
- **`require_partition_filter = TRUE` on history**: protects every consumer from accidental full-table scans without anyone having to think about it.
- **JSONL over Parquet**: native JSON columns serialize cleanly to NEWLINE_DELIMITED_JSON; price strings survive into `BIGNUMERIC` without float round-trips.
- **ADC instead of service-account key files**: Cloud Run's identity is the auth.
- **UUID `run_id`**: BigQuery has no auto-increment; UUID also matches the GCS staging-prefix layout.
- **Empty-discovery safeguard**: if AWS returns zero targets, the loader refuses to swap the live table (`RuntimeError`) and the failure is recorded in `pricing_runs`.
