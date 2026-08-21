# E-Commerce Lakehouse — S3, Delta Lake, Glue, Step Functions & Athena

A production-style Lakehouse for e-commerce transactional data. Raw CSVs dropped into S3
are validated, deduplicated, and upserted into partitioned Delta Lake tables by AWS Glue,
orchestrated end-to-end by Step Functions, and exposed for ad-hoc analytics through
Athena via the Glue Data Catalog. All infrastructure is provisioned with Terraform.

See [`docs/PROJECT_REPORT.md`](docs/PROJECT_REPORT.md) for the design rationale and the
results obtained by deploying this to a real AWS account and driving it end-to-end,
including a deliberate failure/rejection-path test.

## Architecture

```
S3 (raw/<dataset>/)
   │  s3:PutObject
   ▼
EventBridge rule ──▶ Step Functions execution
   │
   ├─ DetermineDataset (Choice on key prefix) ──▶ sets job/table context for the dataset
   │
   ├─ ProcessDataset (Glue PySpark + Delta, .sync)
   │     read raw CSV → dedupe by primary key → validate → MERGE upsert into
   │     lakehouse-dwh/<dataset>/ (Delta, partitioned by date where applicable)
   │     rejected/duplicate rows ──▶ rejected/<dataset>/ (Delta, append, tagged with
   │     the execution name)
   │     └─ on failure ──▶ SNS alert, raw file left in place for re-drive
   │
   ├─ StartCrawler / poll GetCrawler ──▶ refreshes the Data Catalog as native Delta
   │     tables (best-effort: a catalog hiccup doesn't block archiving already-merged data)
   │
   ├─ ValidateWithAthena ──▶ SELECT count(*) presence check (best-effort)
   │
   └─ Archive: copy raw file to archived/<dataset>/, delete from raw/
```

Any failure in the Glue job or the archive step publishes to an SNS topic and leaves the
raw file in `raw/` so it can be re-driven once the underlying issue is fixed. Crawler/Athena
failures are logged but don't fail the execution — the data already merged successfully.

## Repository layout

```
glue_jobs/
  common/
    schemas.py        raw CSV schemas + primary key / partition metadata per dataset
    validation.py      null/type/range checks, dedup, referential-integrity helpers
    delta_io.py        Delta merge/upsert, FK lookup, rejected-record writer
  process_products.py       Glue PySpark job — products
  process_orders.py         Glue PySpark job — orders
  process_order_items.py    Glue PySpark job — order_items
step_functions/
  state_machine.asl.json.tftpl   Step Functions definition (rendered by Terraform)
terraform/
  *.tf                       S3, IAM, Glue jobs + crawler, EventBridge, Athena, SNS,
                             Step Functions
tests/
  test_validation.py, test_delta_io.py   pytest + local PySpark/Delta session
sample_data/
  products.csv, orders.csv, order_items.csv   sample data, converted from Data/ (see below)
Data/
  instructor-provided source files (products.csv, orders_apr_2025.xlsx, order_items_apr_2025.xlsx)
```

## Design decisions

**Schema.** Each dataset has a fixed `StructType` in `common/schemas.py`, enforced on read
rather than inferred — a malformed raw file fails with a clear type error instead of
silently producing an all-string DataFrame.

**Primary keys & dedup.** `product_id` / `order_id` / `id` are the primary keys for
products/orders/order_items respectively. Within a single raw file, duplicates are
resolved by `dedupe_keep_first` (a `ROW_NUMBER()` window), keeping the most recent
`order_timestamp` for orders. Across files/runs, the Delta `MERGE` (`whenMatchedUpdateAll`
+ `whenNotMatchedInsertAll`) makes every load idempotent — reprocessing the same file, or
a corrected re-upload of the same keys, converges rather than duplicating.

**Partitioning.** `orders` and `order_items` are partitioned by `date` — the natural
query-pruning dimension for time-series transactional data, and a column already present
in the source rather than a derived one. `products` is a small, slowly-changing dimension
table and isn't partitioned; it's small enough that a full scan (or broadcast join) is
cheap.

**Validation rules** (`common/validation.py`), rejected rows are tagged with a
`reject_reasons` string and appended to `rejected/<dataset>/` (a Delta table) rather than
dropped silently:
- no null primary identifiers, foreign keys, or other required fields
- `order_timestamp`/`date` must parse; `total_amount` must be non-negative
- `order_items.reordered` must be 0/1; `add_to_cart_order` must be positive;
  `days_since_prior_order` may be null (a customer's first order has no prior gap) but
  not negative
- **referential integrity**: `order_items.order_id`/`product_id` are checked against the
  orders/products Delta tables already on disk, via a left-anti join. If the parent table
  doesn't exist yet (first-ever run) the check is skipped rather than rejecting every row.
  A late-arriving `order_items` file relative to its `orders` file will have those rows
  rejected as `unknown_order_id` — re-driving rejected records once the parent lands is a
  manual follow-up, out of scope here.

**Orchestration.** One state machine handles all three datasets: a `Choice` state routes
on the S3 key's `raw/<dataset>/` prefix into a `Pass` state that sets the job name, Delta
path(s), and Athena table name for that dataset, then a single shared `ProcessDataset` →
`StartCrawler`/poll → `ValidateWithAthena` → archive chain runs for all three. This avoids
tripling the state machine definition for what's otherwise identical branching/failure
logic. Glue's `.sync` integration blocks the state machine until the job run finishes;
`TimeoutSeconds` and a `Retry` on `Glue.ConcurrentRunsExceededException` are set on that
task, and every S3/Glue/Athena task has a `Catch` routing to an SNS alert.

## Data note

The instructor-provided sample data (`Data/`) ships `orders_apr_2025.xlsx` and
`order_items_apr_2025.xlsx` as Excel, but the pipeline's raw-zone contract is CSV-only (per
the brief). `tools/convert_samples_to_csv.py` converts all three files into
`sample_data/*.csv` once, locally. (Named `sample_data/`, not `data/` — this repo also has
a `Data/` folder, and `data`/`Data` collide on case-insensitive filesystems, e.g. default
Windows/WSL mounts and macOS.)

```bash
pip install pandas openpyxl
python tools/convert_samples_to_csv.py
```

## Prerequisites

- Terraform >= 1.5, AWS credentials with permission to create S3, Glue, IAM, Step
  Functions, EventBridge, Athena and SNS resources.
- Python 3.11 (or matching Glue's runtime) for running tests locally; `requirements-dev.txt`
  pulls in `pyspark`, `delta-spark`, `pytest`.

## Deploying

### 0. One-time: bootstrap remote state

Terraform state for the main config is stored remotely in S3 (with DynamoDB locking) so
that CI runs, local applies, and hotfixes all see the same real state instead of each
starting from empty. That S3 bucket/table has to exist before the main config can use it
as its own backend, so it's provisioned once via a separate, local-state config:

```bash
cd terraform/bootstrap
terraform init
terraform apply
terraform output -raw state_bucket_name   # note this
terraform output -raw lock_table_name     # and this
cd ..
cp backend.hcl.example backend.hcl        # fill in the two values above
```

Skip this if the bucket/table already exist for your environment — just fill in
`backend.hcl` with the existing names.

### 1. Deploy the main stack

```bash
cd terraform
terraform init -backend-config=backend.hcl
cp terraform.tfvars.example terraform.tfvars   # set alert_email at minimum
terraform plan
terraform apply
```

This creates the S3 bucket (with all raw/rejected/lakehouse-dwh/archived/scripts
prefixes), uploads the Glue job scripts and the `common/` package (zipped, wired via
`--extra-py-files`), the three Glue jobs, the Delta-aware crawler, the Glue Data Catalog
database, the Athena workgroup, the SNS topic, the Step Functions state machine, and the
EventBridge rule that triggers it on `raw/{products,orders,order_items}/` uploads.

Confirm the SNS email subscription (check your inbox) before failures will actually notify
you.

## Running the pipeline

```bash
BUCKET=$(terraform -chdir=terraform output -raw bucket_name)
aws s3 cp sample_data/products.csv "s3://${BUCKET}/raw/products/products.csv"
aws s3 cp sample_data/orders.csv "s3://${BUCKET}/raw/orders/orders.csv"
aws s3 cp sample_data/order_items.csv "s3://${BUCKET}/raw/order_items/order_items.csv"
```

Each upload triggers its own state machine execution. Upload `orders.csv` before
`order_items.csv` so the referential-integrity check against `orders` has something to
check against. Watch executions via the Step Functions console, or:

```bash
aws stepfunctions list-executions \
  --state-machine-arn "$(terraform -chdir=terraform output -raw state_machine_arn)" \
  --max-results 5
```

On success you should see, per dataset:
1. `s3://${BUCKET}/lakehouse-dwh/<dataset>/` — the Delta table (partitioned by `date` for
   orders/order_items)
2. The Data Catalog database (`terraform output glue_database_name`) populated with a
   native Delta table per dataset, queryable from Athena
3. `s3://${BUCKET}/archived/<dataset>/<filename>` and the original removed from `raw/`

To exercise the failure/rejection path, upload a CSV with a null primary key or an
unparseable timestamp — the bad rows land in `rejected/<dataset>/` (with `reject_reasons`)
while valid rows in the same file still load normally.

## Querying with Athena

```sql
SELECT department, count(*) AS n
FROM ecommerce_lakehouse.products
GROUP BY department
ORDER BY n DESC;

SELECT date, count(*) AS orders, sum(total_amount) AS revenue
FROM ecommerce_lakehouse.orders
GROUP BY date
ORDER BY date;
```

## Tests

```bash
pip install -r requirements-dev.txt
pytest tests/ -q
```

Exercises `common/validation.py` and `common/delta_io.py` against a real local
PySpark + Delta Lake session (no AWS needed) — schema-splitting rules, null-handling edge
cases (e.g. a null `days_since_prior_order` is valid, a null `reordered` is not), the FK
rejection path, and the create-then-merge-upsert Delta lifecycle.

## CI/CD

`.github/workflows/lakehouse-ci.yml` runs on every PR/push to `main`: ruff lint, the pytest
suite above, a dummy-value render-and-parse check of the Step Functions ASL template, and
`terraform fmt -check` + `terraform validate`.

`.github/workflows/lakehouse-terraform.yml` runs `terraform plan` on PRs touching
`terraform/`, `step_functions/`, or `glue_jobs/`, and supports a manual `apply` via
`workflow_dispatch` gated behind a GitHub Environment named `aws` (add required reviewers
there for a human approval gate — note this requires a paid GitHub plan for private repos).

Both `plan` and `apply` jobs initialize against the remote backend from §0 above using
repo **variables** (Settings → Secrets and variables → Actions → Variables tab, not
secrets — these aren't sensitive):

| Variable | Value |
|---|---|
| `TF_STATE_BUCKET` | `terraform -chdir=terraform/bootstrap output -raw state_bucket_name` |
| `TF_STATE_LOCK_TABLE` | `terraform -chdir=terraform/bootstrap output -raw lock_table_name` |
| `TF_STATE_REGION` | optional, defaults to `eu-west-1` if unset |

Without these set, `terraform init` in CI falls back to an empty local state per run —
the same limitation this backend was added to fix.
