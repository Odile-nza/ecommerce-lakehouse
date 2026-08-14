"""
Glue PySpark job: process_order_items

Reads one raw order_items CSV, deduplicates by `id` (most recent
`order_timestamp` wins, matching process_orders' tie-break), validates it
(including referential integrity against the orders/products Delta tables),
parses `order_timestamp`/`date`, and upserts the valid rows into the
`order_items` Delta table, partitioned by `date`.

An order_item whose `order_id`/`product_id` isn't found in the
corresponding dimension table is rejected as `unknown_order_id` /
`unknown_product_id` rather than silently accepted — this can legitimately
happen for a late-arriving order_items file relative to its orders file;
re-driving the rejected records once the parent row lands is a manual
follow-up, out of scope for this pipeline.

Required Glue job arguments:
    --JOB_NAME
    --RAW_BUCKET          source bucket containing the raw file
    --RAW_KEY             key of the raw file to process
    --EXECUTION_NAME      Step Functions execution name (tags rejected records)
    --REJECTED_PATH       s3:// prefix to append rejected records under
    --DELTA_PATH          s3:// path of the order_items Delta table
    --ORDERS_DELTA_PATH   s3:// path of the orders Delta table (FK check)
    --PRODUCTS_DELTA_PATH s3:// path of the products Delta table (FK check)

Must be launched with `--datalake-formats delta` so Glue wires up the Delta
catalog/session extensions.
"""
import sys

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from common.delta_io import merge_upsert, read_existing_keys, write_rejected
from common.schemas import TABLES
from common.validation import dedupe_keep_first, validate_order_items
from pyspark.context import SparkContext
from pyspark.sql import functions as F

args = getResolvedOptions(
    sys.argv,
    [
        "JOB_NAME",
        "RAW_BUCKET",
        "RAW_KEY",
        "EXECUTION_NAME",
        "REJECTED_PATH",
        "DELTA_PATH",
        "ORDERS_DELTA_PATH",
        "PRODUCTS_DELTA_PATH",
    ],
)

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

TABLE = TABLES["order_items"]


def log(msg):
    print(f"[process_order_items] {msg}")


def main():
    raw_path = f"s3://{args['RAW_BUCKET']}/{args['RAW_KEY']}"
    log(f"reading {raw_path}")
    raw = spark.read.option("header", "true").schema(TABLE["schema"]).csv(raw_path)
    raw_count = raw.count()
    log(f"read {raw_count} rows")
    if raw_count == 0:
        raise ValueError(f"no rows found in {raw_path}")

    deduped, batch_duplicates = dedupe_keep_first(
        raw, [TABLE["primary_key"]], order_by=[F.col("order_timestamp").desc()]
    )

    valid_order_ids = read_existing_keys(spark, args["ORDERS_DELTA_PATH"], "order_id")
    valid_product_ids = read_existing_keys(spark, args["PRODUCTS_DELTA_PATH"], "product_id")
    if valid_order_ids is None:
        log("orders Delta table not found yet — skipping order_id referential check")
    if valid_product_ids is None:
        log("products Delta table not found yet — skipping product_id referential check")

    valid, invalid = validate_order_items(
        deduped, valid_order_ids=valid_order_ids, valid_product_ids=valid_product_ids
    )

    rejected = batch_duplicates.unionByName(invalid)
    rejected_count = write_rejected(rejected, args["REJECTED_PATH"], args["EXECUTION_NAME"])
    if rejected_count:
        log(f"rejected {rejected_count} rows -> {args['REJECTED_PATH']}")

    valid = valid.withColumn(
        "order_timestamp", F.to_timestamp(F.col("order_timestamp"), "yyyy-MM-dd'T'HH:mm:ss")
    ).withColumn("date", F.to_date(F.col("date"), "yyyy-MM-dd"))

    valid_count = valid.count()
    if valid_count == 0:
        raise ValueError("no valid rows remain after validation")

    merge_upsert(spark, valid, args["DELTA_PATH"], [TABLE["primary_key"]], TABLE["partition_by"])
    log(f"upserted {valid_count} rows -> {args['DELTA_PATH']}")


if __name__ == "__main__":
    main()
    job.commit()
