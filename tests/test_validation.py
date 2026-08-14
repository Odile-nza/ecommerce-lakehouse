from common.validation import (
    dedupe_keep_first,
    validate_order_items,
    validate_orders,
    validate_products,
)


def test_validate_products_splits_null_and_blank_fields(spark):
    df = spark.createDataFrame(
        [
            (1, 4, "Books", "Widget"),
            (None, 4, "Books", "Widget"),
            (2, None, "Books", "Widget"),
            (3, 4, "  ", "Widget"),
            (4, 4, "Books", None),
        ],
        ["product_id", "department_id", "department", "product_name"],
    )

    valid, rejected = validate_products(df)

    assert valid.count() == 1
    assert valid.collect()[0]["product_id"] == 1
    assert rejected.count() == 4
    reasons = {row["product_id"]: row["reject_reasons"] for row in rejected.collect()}
    assert reasons[None] == "null_product_id"
    assert reasons[2] == "null_department_id"
    assert reasons[3] == "blank_department"
    assert reasons[4] == "blank_product_name"


def test_validate_orders_rejects_bad_timestamp_and_negative_amount(spark):
    df = spark.createDataFrame(
        [
            (1, 100, 1, "2025-04-01T11:27:00", 20.0, "2025-04-01"),
            (2, 101, 1, "not-a-timestamp", 20.0, "2025-04-01"),
            (3, 102, 1, "2025-04-01T11:27:00", -5.0, "2025-04-01"),
            (4, None, 1, "2025-04-01T11:27:00", 20.0, "2025-04-01"),
        ],
        ["order_num", "order_id", "user_id", "order_timestamp", "total_amount", "date"],
    )

    valid, rejected = validate_orders(df)

    assert valid.count() == 1
    assert rejected.count() == 3
    reasons = {row["order_num"]: row["reject_reasons"] for row in rejected.collect()}
    assert reasons[2] == "invalid_order_timestamp"
    assert reasons[3] == "negative_total_amount"
    assert reasons[4] == "null_order_id"


def test_validate_order_items_allows_null_days_since_prior_order(spark):
    df = spark.createDataFrame(
        [
            (1, 100, 1, None, 50, 1, 0, "2025-04-01T11:27:00", "2025-04-01"),
            (2, 100, 1, 10, 51, 2, 2, "2025-04-01T11:27:00", "2025-04-01"),
        ],
        [
            "id",
            "order_id",
            "user_id",
            "days_since_prior_order",
            "product_id",
            "add_to_cart_order",
            "reordered",
            "order_timestamp",
            "date",
        ],
    )

    valid, rejected = validate_order_items(df)

    assert valid.count() == 1
    assert valid.collect()[0]["id"] == 1
    assert rejected.count() == 1
    assert rejected.collect()[0]["reject_reasons"] == "invalid_reordered"


def test_validate_order_items_rejects_unknown_foreign_keys(spark):
    df = spark.createDataFrame(
        [(1, 100, 1, None, 50, 1, 0, "2025-04-01T11:27:00", "2025-04-01")],
        schema=(
            "id INT, order_id INT, user_id INT, days_since_prior_order INT, "
            "product_id INT, add_to_cart_order INT, reordered INT, "
            "order_timestamp STRING, date STRING"
        ),
    )
    valid_order_ids = spark.createDataFrame([(999,)], ["order_id"])
    valid_product_ids = spark.createDataFrame([(50,)], ["product_id"])

    valid, rejected = validate_order_items(
        df, valid_order_ids=valid_order_ids, valid_product_ids=valid_product_ids
    )

    assert valid.count() == 0
    assert rejected.count() == 1
    assert rejected.collect()[0]["reject_reasons"] == "unknown_order_id"


def test_dedupe_keep_first_reports_duplicates(spark):
    df = spark.createDataFrame([(1, "a"), (1, "b"), (2, "c")], ["id", "value"])

    kept, duplicates = dedupe_keep_first(df, ["id"])

    assert kept.count() == 2
    assert duplicates.count() == 1
    assert duplicates.collect()[0]["reject_reasons"] == "duplicate:id"


def test_dedupe_keep_first_order_by_breaks_ties_by_most_recent(spark):
    from pyspark.sql import functions as F

    df = spark.createDataFrame(
        [(1, "2025-04-01T10:00:00"), (1, "2025-04-01T11:00:00")],
        ["id", "order_timestamp"],
    )

    kept, duplicates = dedupe_keep_first(
        df, ["id"], order_by=[F.col("order_timestamp").desc()]
    )

    assert kept.collect()[0]["order_timestamp"] == "2025-04-01T11:00:00"
    assert duplicates.collect()[0]["order_timestamp"] == "2025-04-01T10:00:00"
