output "bucket_name" {
  description = "S3 bucket holding raw/rejected/lakehouse-dwh/archived data and scripts"
  value       = aws_s3_bucket.this.id
}

output "glue_database_name" {
  value = aws_glue_catalog_database.lakehouse.name
}

output "state_machine_arn" {
  value = aws_sfn_state_machine.this.arn
}

output "athena_workgroup" {
  value = aws_athena_workgroup.lakehouse.name
}

output "sns_topic_arn" {
  description = "Confirm the email subscription before failures will actually notify you"
  value       = aws_sns_topic.pipeline_alerts.arn
}
