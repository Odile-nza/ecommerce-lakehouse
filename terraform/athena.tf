resource "aws_athena_workgroup" "lakehouse" {
  name = "${var.project_name}-workgroup"

  configuration {
    result_configuration {
      output_location = "s3://${aws_s3_bucket.this.id}/athena-results/"
    }
  }
}
