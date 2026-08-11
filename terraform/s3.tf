resource "random_id" "bucket_suffix" {
  byte_length = 4
}

resource "aws_s3_bucket" "this" {
  bucket = "${var.bucket_name_prefix}-${random_id.bucket_suffix.hex}"

  # Convenience for a dev/coursework project so `terraform destroy` doesn't
  # get blocked by leftover objects. Remove for a bucket holding real data.
  force_destroy = true
}

resource "aws_s3_bucket_versioning" "this" {
  bucket = aws_s3_bucket.this.id
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_public_access_block" "this" {
  bucket                  = aws_s3_bucket.this.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# Required so the s3:PutObject events under raw/ reach the EventBridge rule
# that triggers the state machine (see eventbridge.tf).
resource "aws_s3_bucket_notification" "eventbridge" {
  bucket      = aws_s3_bucket.this.id
  eventbridge = true
}

resource "aws_s3_object" "prefixes" {
  for_each = toset(local.prefixes)
  bucket   = aws_s3_bucket.this.id
  key      = each.value
}

resource "aws_s3_object" "process_products_script" {
  bucket = aws_s3_bucket.this.id
  key    = "scripts/process_products.py"
  source = "${path.module}/../glue_jobs/process_products.py"
  etag   = filemd5("${path.module}/../glue_jobs/process_products.py")
}

resource "aws_s3_object" "process_orders_script" {
  bucket = aws_s3_bucket.this.id
  key    = "scripts/process_orders.py"
  source = "${path.module}/../glue_jobs/process_orders.py"
  etag   = filemd5("${path.module}/../glue_jobs/process_orders.py")
}

resource "aws_s3_object" "process_order_items_script" {
  bucket = aws_s3_bucket.this.id
  key    = "scripts/process_order_items.py"
  source = "${path.module}/../glue_jobs/process_order_items.py"
  etag   = filemd5("${path.module}/../glue_jobs/process_order_items.py")
}

resource "aws_s3_object" "common_pkg" {
  bucket = aws_s3_bucket.this.id
  key    = "scripts/common.zip"
  source = data.archive_file.common_pkg.output_path
  etag   = data.archive_file.common_pkg.output_md5
}

# Sample raw data — upload manually (or via `aws s3 cp`) to actually trigger
# the pipeline; see the README. Not seeded here so `terraform apply` doesn't
# double as a pipeline trigger.
