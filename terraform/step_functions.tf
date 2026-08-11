resource "aws_cloudwatch_log_group" "state_machine" {
  name              = "/aws/vendedlogs/states/${var.project_name}"
  retention_in_days = 30
}

resource "aws_sfn_state_machine" "this" {
  name     = var.project_name
  role_arn = aws_iam_role.state_machine.arn

  definition = templatefile("${path.module}/../step_functions/state_machine.asl.json.tftpl", {
    bucket_name          = aws_s3_bucket.this.id
    sns_topic_arn        = aws_sns_topic.pipeline_alerts.arn
    products_job_name    = aws_glue_job.process_products.name
    orders_job_name      = aws_glue_job.process_orders.name
    order_items_job_name = aws_glue_job.process_order_items.name
    crawler_name         = aws_glue_crawler.lakehouse_dwh.name
    glue_database        = aws_glue_catalog_database.lakehouse.name
    athena_workgroup     = aws_athena_workgroup.lakehouse.name
    products_table       = local.products_table
    orders_table         = local.orders_table
    order_items_table    = local.order_items_table
  })

  logging_configuration {
    log_destination        = "${aws_cloudwatch_log_group.state_machine.arn}:*"
    include_execution_data = true
    level                  = "ALL"
  }
}
