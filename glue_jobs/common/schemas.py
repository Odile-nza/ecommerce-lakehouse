"""
Raw-zone CSV schemas and per-dataset table metadata.

Schemas are enforced on read (rather than inferred) so a malformed raw file
fails fast with a clear type error instead of silently producing an
all-string DataFrame.
"""
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

PRODUCTS_SCHEMA = StructType(
    [
        StructField("product_id", IntegerType(), nullable=True),
        StructField("department_id", IntegerType(), nullable=True),
        StructField("department", StringType(), nullable=True),
        StructField("product_name", StringType(), nullable=True),
    ]
)

ORDERS_SCHEMA = StructType(
    [
        StructField("order_num", IntegerType(), nullable=True),
        StructField("order_id", IntegerType(), nullable=True),
        StructField("user_id", IntegerType(), nullable=True),
        StructField("order_timestamp", StringType(), nullable=True),
        StructField("total_amount", DoubleType(), nullable=True),
        StructField("date", StringType(), nullable=True),
    ]
)

ORDER_ITEMS_SCHEMA = StructType(
    [
        StructField("id", IntegerType(), nullable=True),
        StructField("order_id", IntegerType(), nullable=True),
        StructField("user_id", IntegerType(), nullable=True),
        StructField("days_since_prior_order", IntegerType(), nullable=True),
        StructField("product_id", IntegerType(), nullable=True),
        StructField("add_to_cart_order", IntegerType(), nullable=True),
        StructField("reordered", IntegerType(), nullable=True),
        StructField("order_timestamp", StringType(), nullable=True),
        StructField("date", StringType(), nullable=True),
    ]
)

# order_timestamp/date arrive as strings so bad values fail validation
# explicitly rather than becoming a silent NULL from a failed schema cast.

TABLES = {
    "products": {
        "schema": PRODUCTS_SCHEMA,
        "primary_key": "product_id",
        "partition_by": [],
    },
    "orders": {
        "schema": ORDERS_SCHEMA,
        "primary_key": "order_id",
        "partition_by": ["date"],
    },
    "order_items": {
        "schema": ORDER_ITEMS_SCHEMA,
        "primary_key": "id",
        "partition_by": ["date"],
    },
}
