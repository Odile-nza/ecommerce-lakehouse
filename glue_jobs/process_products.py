"""
Glue PySpark job: process_products

Reads one raw products CSV, deduplicates by `product_id`, validates it,
and upserts the valid rows into the `products` Delta table. Rejected rows
(duplicates + validation failures) are appended to REJECTED_PATH for
inspection.

Products is a small, slowly-changing dimension table — no partitioning.

Required Glue job arguments:
    --JOB_NAME
    --RAW_BUCKET      source bucket containing the raw file
    --RAW_KEY         key of the raw file to process
    --EXECUTION_NAME  Step Functions execution name (tags rejected records)
    --REJECTED_PATH   s3:// prefix to append rejected records under
    --DELTA_PATH      s3:// path of the products Delta table

Must be launched with `--datalake-formats delta` so Glue wires up the Delta
catalog/session extensions.
"""
import sys

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from common.delta_io import merge_upsert, write_rejected
from common.schemas import TABLES
from common.validation import dedupe_keep_first, validate_products
from pyspark.context import SparkContext

args = getResolvedOptions(
    sys.argv,
    ["JOB_NAME", "RAW_BUCKET", "RAW_KEY", "EXECUTION_NAME", "REJECTED_PATH", "DELTA_PATH"],
)

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

TABLE = TABLES["products"]


def log(msg):
    print(f"[process_products] {msg}")


def main():
    raw_path = f"s3://{args['RAW_BUCKET']}/{args['RAW_KEY']}"
    log(f"reading {raw_path}")
    raw = spark.read.option("header", "true").schema(TABLE["schema"]).csv(raw_path)
    raw_count = raw.count()
    log(f"read {raw_count} rows")
    if raw_count == 0:
        raise ValueError(f"no rows found in {raw_path}")

    deduped, batch_duplicates = dedupe_keep_first(raw, [TABLE["primary_key"]])
    valid, invalid = validate_products(deduped)

    rejected = batch_duplicates.unionByName(invalid)
    rejected_count = write_rejected(rejected, args["REJECTED_PATH"], args["EXECUTION_NAME"])
    if rejected_count:
        log(f"rejected {rejected_count} rows -> {args['REJECTED_PATH']}")

    valid_count = valid.count()
    if valid_count == 0:
        raise ValueError("no valid rows remain after validation")

    merge_upsert(spark, valid, args["DELTA_PATH"], [TABLE["primary_key"]], TABLE["partition_by"])
    log(f"upserted {valid_count} rows -> {args['DELTA_PATH']}")


if __name__ == "__main__":
    main()
    job.commit()
