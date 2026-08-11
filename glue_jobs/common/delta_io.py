"""
Delta Lake read/write helpers shared by all three Glue jobs.

Requires the job to be launched with `--datalake-formats delta` (Glue sets
up the Delta catalog/session extensions automatically from that flag).
"""
from delta.tables import DeltaTable
from pyspark.sql import functions as F


def table_exists(spark, path):
    return DeltaTable.isDeltaTable(spark, path)


def merge_upsert(spark, df, path, merge_keys, partition_by=None):
    """
    Upsert `df` into the Delta table at `path`, keyed by `merge_keys`.
    Creates the table (partitioned by `partition_by`) on first write. Every
    call performs a real MERGE — safe to call repeatedly (idempotent
    reprocessing/backfills), and handles both new rows and corrections to
    previously-loaded rows.
    """
    if not table_exists(spark, path):
        writer = df.write.format("delta").mode("overwrite")
        if partition_by:
            writer = writer.partitionBy(*partition_by)
        writer.save(path)
        return

    target = DeltaTable.forPath(spark, path)
    condition = " AND ".join(f"t.{k} = s.{k}" for k in merge_keys)
    (
        target.alias("t")
        .merge(df.alias("s"), condition)
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )


def read_existing_keys(spark, path, key_column):
    """
    Return a single-column DataFrame of distinct `key_column` values already
    present in the Delta table at `path`, or None if the table doesn't exist
    yet — callers use `is None` to mean "skip the FK check, nothing to
    check against on a first run."
    """
    if not table_exists(spark, path):
        return None
    return spark.read.format("delta").load(path).select(key_column).distinct()


def write_rejected(df, path, batch_id):
    """
    Append rejected records (with `reject_reasons`) under `path`, tagged
    with the triggering execution/run id so each run's rejects are easy to
    isolate without needing a separate manifest.
    """
    count = df.count()
    if count == 0:
        return 0
    df.withColumn("_batch_id", F.lit(batch_id)).write.format("delta").mode("append").option(
        "mergeSchema", "true"
    ).save(path)
    return count
