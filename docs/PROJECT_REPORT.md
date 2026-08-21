
# Project Report — E-Commerce Lakehouse (S3, Delta Lake, Glue, Step Functions, Athena)

This document records what was built, the reasoning behind the key design decisions, and
the empirical results obtained by deploying the stack to a real AWS account and driving
it end-to-end with real data — including a deliberate failure/rejection test. It
complements [`README.md`](../README.md), which covers setup/usage instructions; this
report covers *what happened when we actually ran it*.

- **Repository:** [github.com/Odile-nza/ecommerce-lakehouse](https://github.com/Odile-nza/ecommerce-lakehouse)
- **AWS account:** `825765383386`, region `eu-west-1`
- **Deployed bucket:** `ecommerce-lakehouse-75757245`

---

## 1. Brief

Design and implement a production-grade Lakehouse on AWS for e-commerce transaction data
(products, orders, order_items): ingest raw CSVs from an S3 raw zone, clean/dedupe/validate
with Glue + Delta Lake, write partitioned Delta tables to a processed zone, register them
in the Glue Data Catalog for Athena, archive originals, orchestrate the whole lifecycle
with Step Functions, and automate deployment with GitHub Actions CI/CD. All infrastructure
via Terraform (a standing project requirement).

## 2. What was built

### 2.1 Terraform infrastructure (`terraform/`)

| File | Resources |
|---|---|
| `s3.tf` | S3 bucket (`force_destroy` for coursework convenience), versioning, public-access block, EventBridge notifications, all `raw/`/`rejected/`/`lakehouse-dwh/`/`archived/`/`scripts/`/`athena-results/` prefixes, uploaded Glue scripts + zipped `common/` package |
| `iam.tf` | Roles for: Glue ETL jobs, Glue Crawler, Step Functions execution, EventBridge-to-StepFunctions invocation |
| `glue.tf` | Glue Data Catalog database, 3 Glue PySpark jobs (`process-products`, `process-orders`, `process-order-items`), 1 Delta-aware Glue Crawler covering all three `lakehouse-dwh/` locations |
| `eventbridge.tf` | Rule matching `s3:PutObject` under any `raw/{products,orders,order_items}/` prefix → triggers the state machine |
| `athena.tf` | Athena workgroup with a dedicated results location |
| `sns.tf` | SNS topic + email subscription for pipeline failure alerts |
| `step_functions.tf` | CloudWatch log group + the state machine, rendered from the ASL template |
| `outputs.tf` | Bucket name, Glue database name, state machine ARN, Athena workgroup, SNS topic ARN |
| `bootstrap/main.tf` | Separate root module (its own local state): the S3 bucket + DynamoDB lock table that the main config uses as *its* remote backend |

A remote backend (`terraform/bootstrap/` + a partial `backend "s3" {}` config) was added
after initial deployment to fix a real gap this project hit twice — see
[§6.3](#63-no-remote-terraform-state-found-fixed-after-teardown).

### 2.2 Glue PySpark jobs (`glue_jobs/`)

A shared `common/` package (zipped and attached to every job via `--extra-py-files`) holds:

- **`schemas.py`** — one `StructType` per dataset, enforced on read (not inferred), plus
  primary key and partition-column metadata.
- **`validation.py`** — `dedupe_keep_first` (window-function dedup) and per-dataset
  `validate_*` functions that split a DataFrame into `(valid, rejected)`, tagging rejects
  with a `reject_reasons` string.
- **`delta_io.py`** — `merge_upsert` (create-or-MERGE into a Delta table), `read_existing_keys`
  (for FK lookups), `write_rejected` (append rejects to a Delta table, tagged with the
  triggering execution name).

Three thin job scripts (`process_products.py`, `process_orders.py`, `process_order_items.py`)
wire these together: read raw CSV → dedupe → validate (order_items additionally checks
referential integrity against the orders/products Delta tables) → write rejects → parse
timestamps/dates → merge-upsert into `lakehouse-dwh/<dataset>/`.

### 2.3 Orchestration (`step_functions/state_machine.asl.json.tftpl`)

One state machine handles all three datasets: `DetermineDataset` (Choice on the S3 key's
`raw/<dataset>/` prefix) → a `Pass` state sets per-dataset job name / Delta paths / Athena
table name → the shared chain runs: `ProcessDataset` (Glue `.sync`) → `StartCrawler` + poll
loop → `ValidateWithAthena` (best-effort presence check) → archive raw file → delete from
`raw/`. Any Glue/S3 failure routes to an SNS alert; crawler/Athena failures are logged but
don't block archiving (the merge already succeeded).

### 2.4 CI/CD (`.github/workflows/`)

- **`lakehouse-ci.yml`** (push/PR to `main`): ruff lint, pytest suite (real PySpark + Delta
  session), a dummy-value render-and-parse check of the ASL template, `terraform fmt
  -check` + `terraform validate`.
- **`lakehouse-terraform.yml`**: `terraform plan` on PRs touching `terraform/`/
  `step_functions/`/`glue_jobs/`; manual `terraform apply` via `workflow_dispatch`, gated
  behind a GitHub Environment named `aws`.
- Mirrored, path-filtered copies also exist at the monorepo root, matching the convention
  already established by the sibling "Project 1" ETL project.

### 2.5 Tests (`tests/`)

`test_validation.py` and `test_delta_io.py` run against a real local PySpark 3.4.1 +
delta-spark 2.4.0 session (no AWS calls) — schema-splitting rules, edge cases (a null
`days_since_prior_order` is valid, a null `reordered` is not), the FK-rejection path via
left-anti joins, and the full create-then-merge-upsert Delta lifecycle.

---

## 3. Key design decisions and justification

**Schema enforcement over inference.** Each dataset has a fixed `StructType`
(`common/schemas.py`). A malformed raw file fails fast with a clear type error instead of
silently producing an all-string DataFrame that passes every downstream check vacuously.

**Primary keys & in-file dedup.** `product_id` / `order_id` / `id` are the primary keys.
Duplicates within a single raw file are resolved by `dedupe_keep_first`, a
`ROW_NUMBER() OVER (PARTITION BY <key> ORDER BY ...)` window. Both orders and order_items
break ties by most recent `order_timestamp` (business-meaningful: the latest version of a
record wins). This was initially inconsistent — order_items' dedupe call had no `order_by`
at all, so its tie-break was effectively arbitrary — until the asymmetry was surfaced
during rejection-path testing (§5.2) and fixed (§6.1).

**Idempotent upserts across files/runs.** Regardless of in-file dedup, a Delta `MERGE`
(`whenMatchedUpdateAll` + `whenNotMatchedInsertAll`) makes every load idempotent —
reprocessing the same file, or a corrected re-upload of the same keys, converges instead
of duplicating.

**Partitioning by `date`.** `orders` and `order_items` are partitioned by `date` — the
natural query-pruning dimension for time-series transactional data, and a column already
present in the source. `products` is an unpartitioned, slowly-changing dimension table,
small enough that a full scan (or broadcast join) is cheap.

**Validation rules, with rejects preserved, not dropped.** No null primary/foreign keys;
`order_timestamp`/`date` must parse; `total_amount` ≥ 0; `order_items.reordered` ∈ {0,1};
`add_to_cart_order` > 0; `days_since_prior_order` may be null (a customer's first order has
no prior gap) but not negative; `order_items.order_id`/`product_id` must exist in the
orders/products Delta tables (skipped, not failed, if that table doesn't exist yet — e.g.
first-ever run). Every rejected row is appended to `rejected/<dataset>/` (itself a Delta
table) with a `reject_reasons` string and the triggering execution name, rather than
silently dropped.

**One state machine, not three.** A `Choice`+`Pass` routing layer lets all three datasets
share one `ProcessDataset → StartCrawler → ValidateWithAthena → archive` chain, avoiding
tripling an otherwise-identical definition. The cost of this design showed up directly as
a real bug — see §5.1.

---

## 4. Deployment

1. Pushed the scaffolded repo to GitHub (`Odile-nza/ecommerce-lakehouse`, branch `main`).
2. `lakehouse-ci.yml` ran automatically on push — all 4 jobs (lint, pytest, ASL validate,
   terraform fmt/validate) passed on the first try, in 1m29s total.
3. Attempted to add a GitHub Environment (`aws`) with a required-reviewers approval gate
   for `terraform apply`; **required reviewers is unavailable for private repos on GitHub's
   free tier**, so the environment holds only secrets — the manual `workflow_dispatch`
   trigger itself is the approval step.
4. Added repo/environment secrets (`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`,
   `AWS_SESSION_TOKEN`, `ALERT_EMAIL`).
5. Ran `lakehouse-terraform.yml` via `workflow_dispatch` with `action: plan` (22s,
   success), then `action: apply` (41s, success — one benign Node.js-20-deprecation
   warning from the GitHub Actions runtime, not from our code).

This created, in AWS account `825765383386` / `eu-west-1`:

| Resource | Name |
|---|---|
| S3 bucket | `ecommerce-lakehouse-75757245` |
| Glue jobs | `process-products`, `process-orders`, `process-order-items` |
| Glue Crawler | `ecommerce-lakehouse-dwh-crawler` |
| Glue database | `ecommerce_lakehouse` |
| Athena workgroup | `ecommerce-lakehouse-workgroup` |
| SNS topic | `ecommerce-lakehouse-alerts` |
| Step Functions | `ecommerce-lakehouse` |

---

## 5. End-to-end validation against real AWS

### 5.1 Bug found on first live run, fixed, verified

The first two live executions (uploading `products.csv` and `orders.csv`) **both failed
in under 100ms**, before ever invoking Glue:

```
States.Runtime: The JSONPath '$.ctx.ordersDeltaPath' specified for the field
'--ORDERS_DELTA_PATH.$' could not be found in the input '{...}'
```

**Root cause:** the shared `ProcessDataset` task's `Arguments` block references
`$.ctx.ordersDeltaPath` / `$.ctx.productsDeltaPath` unconditionally (needed for
order_items' FK check), but only `SetOrderItemsContext` populated those fields —
`SetProductsContext` and `SetOrdersContext` didn't. Step Functions requires every
referenced `.$` JSONPath to resolve, so any products/orders upload was doomed to fail
instantly; only order_items would ever have worked.

**Fix:** populated `ordersDeltaPath`/`productsDeltaPath` in all three `Pass` states'
`Parameters`, so every context has an identical shape (harmless/unused extra Glue job
arguments for products/orders).

**Deployment note:** because there's no remote Terraform state (§6.3), the fix was applied
directly to the live state machine via `aws stepfunctions update-state-machine` rather
than re-running `terraform apply` from an empty state (which would have attempted to
recreate fixed-name resources like IAM roles and collided with what already existed). The
source fix was committed and pushed separately (`f7b4349`) so the repo matches what's
deployed.

**Verification:** re-uploaded the same two files — both executions now `SUCCEEDED`.

### 5.2 Happy-path run — all three datasets

| Dataset | Source rows | Delta rows after | Rejected | Execution time | Archived | Catalog table |
|---|---:|---:|---:|---:|:---:|---|
| products | 1000 | 1000 | 0 | 2m25s | ✅ | 4 columns, no partition |
| orders | 500 | 500 | 0 | 2m06s | ✅ | partitioned by `date` |
| order_items | 2768 | 2768 | 0 | 4m08s | ✅ | partitioned by `date`, FK-checked |

Row counts were verified by reading the actual Delta parquet output with PyArrow and
comparing against the source CSVs — exact match, zero silent loss, zero unexpected
rejects. `raw/<dataset>/` was empty and `archived/<dataset>/` held the original file after
each run. order_items' run is the longest, consistent with it being the largest file and
the only job doing two additional Delta reads for the FK checks.

One incidental finding: the Glue Crawler creates a **placeholder** catalog entry (0
columns, `delta.lastUpdateVersion: -1`) for `order_items` the moment Terraform
pre-creates the empty `lakehouse-dwh/order_items/` S3 prefix marker — before any real data
or Delta log exists there. Harmless; the first real crawl after data lands replaces it
with the correct schema.

### 5.3 Athena queries (from the README) against live data

```sql
SELECT department, count(*) AS n FROM ecommerce_lakehouse.products GROUP BY department ORDER BY n DESC;
```

| department | n |
|---|---:|
| Home | 184 |
| Sports | 174 |
| Clothing | 169 |
| Books | 166 |
| Toys | 158 |
| Electronics | 149 |

(Sums to 1000 — matches the loaded row count.)

```sql
SELECT date, count(*) AS orders, sum(total_amount) AS revenue FROM ecommerce_lakehouse.orders GROUP BY date ORDER BY date;
```

| date | orders | revenue |
|---|---:|---:|
| 2025-04-01 | 500 | $132,204.46 |

(All 500 sample orders fall on a single calendar day, despite the source file being named
`orders_apr_2025.xlsx` — a property of the sample dataset, not a partitioning bug; confirmed
by checking the distinct `date` values in the source CSV.)

Both queries executed successfully through Athena → Glue Data Catalog → the Delta tables
written by Glue, confirming the full read path works, not just the write path.

### 5.4 Deliberate failure/rejection-path test

Crafted two small CSVs, each with one valid row and six deliberately invalid/duplicate
rows exercising every validation rule not yet hit by the clean sample data, and uploaded
them to trigger real executions.

**`orders_reject_test.csv`** (500 → **501** rows in Delta, exactly +1):

| order_num | order_id | issue | `reject_reasons` |
|---|---|---|---|
| 9001 | 99001 | duplicate of 9007 | `duplicate:order_id` |
| 9002 | *(null)* | missing PK | `null_order_id` |
| 9003 | 99003 | unparseable timestamp | `invalid_order_timestamp` |
| 9004 | 99004 | negative amount | `negative_total_amount` |
| 9005 | 99005 | missing user_id | `null_user_id` |
| 9006 | 99006 | unparseable date | `invalid_date` |
| **9007** | **99001** | — | **merged** (won the dedup tiebreak: later `order_timestamp`) |

**`order_items_reject_test.csv`** (2768 → **2769** rows in Delta, exactly +1):

| id | issue | `reject_reasons` |
|---|---|---|
| 90001 (1st copy) | duplicate of 2nd copy | `duplicate:id` |
| 90002 | `order_id=999999` doesn't exist | `unknown_order_id` |
| 90003 | `product_id=999999` doesn't exist | `unknown_product_id` |
| 90004 | `reordered=5` | `invalid_reordered` |
| 90005 | `add_to_cart_order=0` | `invalid_add_to_cart_order` |
| 90006 | `days_since_prior_order=-3` | `negative_days_since_prior_order` |
| **90001 (2nd copy)** | — | **merged** |

Every rule fired with the correct, specific `reject_reasons` label; exactly one valid row
per file survived and merged; row counts increased by precisely 1 each (verified by
reading the Delta parquet directly) — no double-counting, no silent loss, no false
rejections of the valid rows.

**Finding from this test:** the two datasets' dedup tie-breaks behave differently. Orders'
`dedupe_keep_first` call passes `order_by=[F.col("order_timestamp").desc()]`, so the
*most recent* duplicate wins (9007, timestamp `11:00`, beat 9001's `10:00`). order_items'
call passes no `order_by`, so among its two `id=90001` copies, the *earlier*-timestamped
one happened to survive — essentially arbitrary, not a designed "most recent wins" rule.
This is consistent with the code as documented, but is an inconsistency worth resolving
(see §6.1).

---

## 6. Known limitations / follow-ups

### 6.1 Dedup tie-break inconsistency (found and fixed)

`process_order_items.py`'s `dedupe_keep_first` call didn't pass an `order_by`, unlike
`process_orders.py`, so the surviving row among duplicate `id`s was effectively arbitrary
rather than "most recent wins." Fixed by adding
`order_by=[F.col("order_timestamp").desc()]` to the order_items dedupe call, matching
orders' tie-break exactly — dedup happens before the timestamp column is parsed from
string to `TimestampType` in both jobs, so the comparison semantics are identical. Added a
regression test (`test_dedupe_keep_first_order_by_breaks_ties_by_most_recent`) covering
the `order_by` parameter directly, since neither job script's dedup call had been unit
tested at that level of specificity before — which is how the inconsistency went unnoticed
until the rejection-path test in §5.4 surfaced it empirically.

### 6.2 GitHub Environment approval gate unavailable

Required-reviewers protection rules for a GitHub Environment require a paid plan for
private repositories. The `aws` environment currently only gates secrets; the manual
`workflow_dispatch` + explicit `action: apply` selection is the de facto approval step.

### 6.3 No remote Terraform state (found, fixed after teardown)

`terraform/versions.tf` originally had no `backend` block, so state existed only inside
whichever runner executed `apply` and was discarded afterward. This worked for the initial
`apply` (empty state → create everything) but meant a second `apply` from a fresh runner
would attempt to recreate fixed-name resources (IAM roles, Glue jobs, the Glue database,
the SNS topic, etc.) and collide with what's already deployed. Both the Step Functions
template bug fix (§5.1) and the eventual full teardown (§7) had to bypass Terraform
entirely — via direct `aws stepfunctions update-state-machine` and a hand-written AWS CLI
deletion script, respectively — specifically because Terraform couldn't be trusted to know
the real state of the account.

**Fixed** by adding `terraform/bootstrap/` — a small, separate, local-state config that
provisions an S3 bucket (versioned, encrypted) and a DynamoDB lock table — and wiring
`terraform/versions.tf` to use them as a partial `backend "s3" {}` config (bucket/key/table
supplied at `init` time via `backend.hcl` or, in CI, via `-backend-config` flags built from
GitHub Actions repo variables). This was added *after* the AWS infrastructure was already
torn down (§7), so it has been validated with `terraform validate`/`init -backend=false`
but not yet exercised against a live `apply` — the next deployment of this project will be
the first real test of the backend actually persisting state across runs.

### 6.4 Case-insensitive filesystem collision (caught before deployment)

The scaffolding step discovered that `data/` (a working directory the project needed for
converted sample CSVs) and `Data/` (the instructor-provided source folder) resolve to the
**same directory** on case-insensitive filesystems — the default for Windows/WSL drive
mounts and macOS. An early `mv` briefly clobbered files in `Data/` as a result; the
original files were restored from the already-converted copies, and the working directory
was renamed to `sample_data/` to make the collision impossible going forward.

---

## 7. Teardown

Once end-to-end validation (§5) and the dedup fix (§6.1) were confirmed working, all AWS
resources were deliberately destroyed to avoid ongoing cost — this was a validation
exercise, not a long-lived deployment.

**`terraform destroy` was not used**, for the same reason noted in §6.3: with no state
persisted anywhere, it would have run against an empty state and silently done nothing
while every real resource stayed live. Instead, every resource was deleted directly via
AWS CLI, in dependency order, with each step verified before moving to the next:

1. S3 bucket — versioning meant deleting all 58 object versions and 5 delete markers
   first (`list-object-versions` + `delete-objects`), then `delete-bucket`
2. Step Functions state machine — confirmed gone via `describe-state-machine` returning
   `StateMachineDoesNotExist`
3. EventBridge rule + target (the real target ID, `terraform-<timestamp>...`, differed
   from the `1` initially assumed — caught by listing targets before removing them)
4. All 3 Glue jobs
5. Glue Crawler and the Glue Data Catalog database
6. Athena workgroup (`--recursive-delete-option`, to also clear saved query history)
7. SNS topic
8. All 4 IAM roles — inline policies deleted and the one attached managed policy
   (`AWSGlueServiceRole`) detached before each role itself could be deleted
9. The Step Functions CloudWatch log group

The AWS CLI session used short-lived STS credentials that expired mid-teardown (immediately
after step 2); teardown resumed cleanly from step 3 once refreshed, since each step was
verified independently rather than assumed. Project 1's unrelated resources (the `primary`
Athena workgroup, the `pipeline-alerts` SNS topic) were confirmed still present and
untouched. Every deletion was independently verified with a `get`/`describe`/`list` call
returning "not found" or an empty result — not just "the delete command returned exit 0."

---

## 8. Summary

Every deliverable in the project brief was implemented and — unlike a purely
paper/plan-stage submission — **exercised against a real, deployed AWS account**: raw CSV
ingestion, Glue+Delta cleaning/dedup/validation, partitioned Delta tables, Glue Data
Catalog registration, Athena querying, archiving, Step Functions orchestration with
failure branching, and GitHub Actions CI/CD. Two real bugs were found and fixed through
this process that would not have surfaced from local unit tests alone: the Step Functions
JSONPath routing bug (§5.1), specific to how the three dataset branches share one
`ProcessDataset` task, and the order_items dedup tie-break inconsistency (§6.1), caught by
the deliberate rejection-path test rather than by code review. A third structural gap — no
remote Terraform state (§6.3) — was identified through the operational pain of working
around it twice (a hotfix and a full teardown) and fixed afterward. Together these are a
good illustration of why "deploy and drive it for real" testing catches a different class
of bug than unit tests targeting application logic in isolation.
