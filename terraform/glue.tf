resource "aws_glue_catalog_database" "lakehouse" {
  name = var.glue_database_name
}

locals {
  # Shared across the three PySpark ETL jobs.
  glue_common_arguments = {
    "--datalake-formats" = "delta"
    "--extra-py-files"   = "s3://${aws_s3_bucket.this.id}/scripts/common.zip"
  }
}

resource "aws_glue_job" "process_products" {
  name     = local.products_job_name
  role_arn = aws_iam_role.glue_etl.arn

  command {
    name            = "glueetl"
    script_location = "s3://${aws_s3_bucket.this.id}/scripts/process_products.py"
    python_version  = "3"
  }

  glue_version      = "4.0"
  worker_type       = "G.1X"
  number_of_workers = 2
  timeout           = 15

  default_arguments = local.glue_common_arguments

  execution_property {
    max_concurrent_runs = 1
  }

  depends_on = [aws_s3_object.process_products_script, aws_s3_object.common_pkg]
}

resource "aws_glue_job" "process_orders" {
  name     = local.orders_job_name
  role_arn = aws_iam_role.glue_etl.arn

  command {
    name            = "glueetl"
    script_location = "s3://${aws_s3_bucket.this.id}/scripts/process_orders.py"
    python_version  = "3"
  }

  glue_version      = "4.0"
  worker_type       = "G.1X"
  number_of_workers = 2
  timeout           = 15

  default_arguments = local.glue_common_arguments

  execution_property {
    max_concurrent_runs = 1
  }

  depends_on = [aws_s3_object.process_orders_script, aws_s3_object.common_pkg]
}

resource "aws_glue_job" "process_order_items" {
  name     = local.order_items_job_name
  role_arn = aws_iam_role.glue_etl.arn

  command {
    name            = "glueetl"
    script_location = "s3://${aws_s3_bucket.this.id}/scripts/process_order_items.py"
    python_version  = "3"
  }

  glue_version      = "4.0"
  worker_type       = "G.1X"
  number_of_workers = 2
  timeout           = 20

  default_arguments = local.glue_common_arguments

  execution_property {
    max_concurrent_runs = 1
  }

  depends_on = [aws_s3_object.process_order_items_script, aws_s3_object.common_pkg]
}

# Registers each Delta location in lakehouse-dwh/ as a native Delta table in
# the Data Catalog, so Athena can query it directly with no separate manifest
# generation step.
resource "aws_glue_crawler" "lakehouse_dwh" {
  name          = local.crawler_name
  role          = aws_iam_role.glue_crawler.arn
  database_name = aws_glue_catalog_database.lakehouse.name

  delta_target {
    delta_tables              = ["s3://${aws_s3_bucket.this.id}/lakehouse-dwh/products/"]
    write_manifest            = false
    create_native_delta_table = true
  }
  delta_target {
    delta_tables              = ["s3://${aws_s3_bucket.this.id}/lakehouse-dwh/orders/"]
    write_manifest            = false
    create_native_delta_table = true
  }
  delta_target {
    delta_tables              = ["s3://${aws_s3_bucket.this.id}/lakehouse-dwh/order_items/"]
    write_manifest            = false
    create_native_delta_table = true
  }

  schema_change_policy {
    update_behavior = "UPDATE_IN_DATABASE"
    delete_behavior = "LOG"
  }
}
