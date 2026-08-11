from common.delta_io import (
    merge_upsert,
    read_existing_keys,
    table_exists,
    write_rejected,
)


def test_merge_upsert_creates_then_upserts(spark, tmp_path):
    path = str(tmp_path / "products")

    first = spark.createDataFrame([(1, "Books"), (2, "Toys")], ["product_id", "department"])
    assert not table_exists(spark, path)
    merge_upsert(spark, first, path, ["product_id"])
    assert table_exists(spark, path)

    rows = {r["product_id"]: r["department"] for r in spark.read.format("delta").load(path).collect()}
    assert rows == {1: "Books", 2: "Toys"}

    # Second batch: corrects product_id=1, adds product_id=3.
    second = spark.createDataFrame([(1, "Electronics"), (3, "Garden")], ["product_id", "department"])
    merge_upsert(spark, second, path, ["product_id"])

    rows = {r["product_id"]: r["department"] for r in spark.read.format("delta").load(path).collect()}
    assert rows == {1: "Electronics", 2: "Toys", 3: "Garden"}


def test_read_existing_keys_returns_none_for_missing_table(spark, tmp_path):
    assert read_existing_keys(spark, str(tmp_path / "does-not-exist"), "product_id") is None


def test_read_existing_keys_returns_distinct_values(spark, tmp_path):
    path = str(tmp_path / "orders")
    df = spark.createDataFrame([(1,), (1,), (2,)], ["order_id"])
    merge_upsert(spark, df, path, ["order_id"])

    keys = {r["order_id"] for r in read_existing_keys(spark, path, "order_id").collect()}
    assert keys == {1, 2}


def test_write_rejected_appends_and_returns_count(spark, tmp_path):
    path = str(tmp_path / "rejected" / "products")
    rejected = spark.createDataFrame(
        [(None, "null_product_id")], schema="product_id INT, reject_reasons STRING"
    )

    count = write_rejected(rejected, path, "test-execution-1")
    assert count == 1
    written = spark.read.format("delta").load(path).collect()
    assert written[0]["_batch_id"] == "test-execution-1"


def test_write_rejected_is_noop_for_empty_dataframe(spark, tmp_path):
    path = str(tmp_path / "rejected" / "empty")
    empty = spark.createDataFrame([], "product_id INT, reject_reasons STRING")

    count = write_rejected(empty, path, "test-execution-2")
    assert count == 0
    assert not table_exists(spark, path)
