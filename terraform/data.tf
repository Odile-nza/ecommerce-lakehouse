data "aws_caller_identity" "current" {}

data "aws_region" "current" {}

data "archive_file" "common_pkg" {
  type        = "zip"
  output_path = "${path.module}/build/common.zip"

  dynamic "source" {
    for_each = fileset("${path.module}/../glue_jobs/common", "*.py")
    content {
      content  = file("${path.module}/../glue_jobs/common/${source.value}")
      filename = "common/${source.value}"
    }
  }
}
