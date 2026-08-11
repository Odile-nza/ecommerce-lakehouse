"""
Reusable validation and deduplication helpers shared by all three Glue jobs.

Every `validate_*` function returns `(valid_df, rejected_df)`. `rejected_df`
carries every original column plus `reject_reasons` (comma-joined rule
names) so rejected records can be written out for inspection without losing
any information.
"""
from pyspark.sql import functions as F


def _split_valid_rejected(df, checks):
    """
    checks: list of (rule_name, boolean_column) — True means the row PASSES
    that rule. `concat_ws` skips NULL arguments (rather than nulling the
    whole result, unlike `concat`), so passing rules simply contribute
    nothing to `_reject_reasons`.
    """
    reason_cols = [F.when(~cond, F.lit(name)) for name, cond in checks]
    df = df.withColumn("_reject_reasons", F.concat_ws(",", *reason_cols))
    valid_df = df.filter(F.col("_reject_reasons") == "").drop("_reject_reasons")
    rejected_df = df.filter(F.col("_reject_reasons") != "").withColumnRenamed(
        "_reject_reasons", "reject_reasons"
    )
    return valid_df, rejected_df


def dedupe_keep_first(df, keys, order_by=None):
    """
    Drop duplicate rows sharing the same `keys`, keeping one row per key.
    Returns (kept_df, duplicates_df) where duplicates_df has `reject_reasons`
    set to "duplicate:<keys>".

    `order_by` (list of Column, optional) breaks ties deterministically
    (e.g. most recent order_timestamp wins); without it, ties are broken
    arbitrarily but deterministically-per-run via row order.
    """
    from pyspark.sql import Window

    window = Window.partitionBy(*keys).orderBy(*(order_by or [F.lit(1)]))
    ranked = df.withColumn("_rn", F.row_number().over(window))
    kept = ranked.filter(F.col("_rn") == 1).drop("_rn")
    duplicates = (
        ranked.filter(F.col("_rn") > 1)
        .drop("_rn")
        .withColumn("reject_reasons", F.lit(f"duplicate:{','.join(keys)}"))
    )
    return kept, duplicates


def validate_products(df):
    checks = [
        ("null_product_id", F.col("product_id").isNotNull()),
        ("null_department_id", F.col("department_id").isNotNull()),
        ("blank_department", F.trim(F.col("department")).isNotNull() & (F.trim(F.col("department")) != "")),
        ("blank_product_name", F.trim(F.col("product_name")).isNotNull() & (F.trim(F.col("product_name")) != "")),
    ]
    return _split_valid_rejected(df, checks)


def validate_orders(df):
    """
    Leaves `order_timestamp`/`date` as their original string type on both
    outputs — parsing to timestamp/date happens once, on the final valid
    set, in the calling job. Keeping the two outputs schema-identical here
    matters most for `validate_order_items`, which unions rejected rows
    computed at different stages.
    """
    parsed_ts = F.to_timestamp(F.col("order_timestamp"), "yyyy-MM-dd'T'HH:mm:ss")
    parsed_date = F.to_date(F.col("date"), "yyyy-MM-dd")
    checks = [
        ("null_order_id", F.col("order_id").isNotNull()),
        ("null_user_id", F.col("user_id").isNotNull()),
        ("invalid_order_timestamp", parsed_ts.isNotNull()),
        ("invalid_date", parsed_date.isNotNull()),
        ("negative_total_amount", F.col("total_amount").isNotNull() & (F.col("total_amount") >= 0)),
    ]
    return _split_valid_rejected(df, checks)


def validate_order_items(df, valid_order_ids=None, valid_product_ids=None):
    """
    `valid_order_ids` / `valid_product_ids` (single-column DataFrames,
    optional) enable referential-integrity checks against the orders/products
    Delta tables already on disk. Omitted when the referenced table doesn't
    exist yet (e.g. first-ever run) — in that case the FK check is skipped
    rather than rejecting every row. As with `validate_orders`,
    `order_timestamp`/`date` are left as strings; the caller parses them
    once on the final valid set.
    """
    parsed_ts = F.to_timestamp(F.col("order_timestamp"), "yyyy-MM-dd'T'HH:mm:ss")
    parsed_date = F.to_date(F.col("date"), "yyyy-MM-dd")
    checks = [
        ("null_id", F.col("id").isNotNull()),
        ("null_order_id", F.col("order_id").isNotNull()),
        ("null_product_id", F.col("product_id").isNotNull()),
        ("invalid_order_timestamp", parsed_ts.isNotNull()),
        ("invalid_date", parsed_date.isNotNull()),
        ("invalid_add_to_cart_order", F.col("add_to_cart_order").isNotNull() & (F.col("add_to_cart_order") > 0)),
        ("invalid_reordered", F.col("reordered").isNotNull() & F.col("reordered").isin(0, 1)),
        (
            "negative_days_since_prior_order",
            F.col("days_since_prior_order").isNull() | (F.col("days_since_prior_order") >= 0),
        ),
    ]
    valid_df, rejected_df = _split_valid_rejected(df, checks)

    if valid_order_ids is not None:
        orphan_orders = valid_df.join(valid_order_ids, on="order_id", how="left_anti").withColumn(
            "reject_reasons", F.lit("unknown_order_id")
        )
        valid_df = valid_df.join(valid_order_ids, on="order_id", how="left_semi")
        rejected_df = rejected_df.unionByName(orphan_orders)

    if valid_product_ids is not None:
        orphan_products = valid_df.join(valid_product_ids, on="product_id", how="left_anti").withColumn(
            "reject_reasons", F.lit("unknown_product_id")
        )
        valid_df = valid_df.join(valid_product_ids, on="product_id", how="left_semi")
        rejected_df = rejected_df.unionByName(orphan_products)

    return valid_df, rejected_df
