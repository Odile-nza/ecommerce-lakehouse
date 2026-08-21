# One-time bootstrap: creates the S3 bucket + DynamoDB lock table used as the
# remote backend for the *main* terraform/ config (../).
#
# This config's own state stays local, deliberately — it's simple, applied
# once, and almost never touched again. Bootstrapping a remote backend's
# storage with that same remote backend is a chicken-and-egg problem, so
# this is the one piece of this project intentionally exempt from the
# "everything is remote state" rule.
#
# Usage (run once per environment, before the main config's first `init`):
#   cd terraform/bootstrap
#   terraform init
#   terraform apply
#   terraform output -raw state_bucket_name
#   terraform output -raw lock_table_name
# Then wire those into ../backend.hcl (see backend.hcl.example) and run
# `terraform init -backend-config=backend.hcl` in ../.

terraform {
  required_version = ">= 1.5"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
    random = {
      source  = "hashicorp/random"
      version = "~> 3.6"
    }
  }
}

variable "aws_region" {
  description = "AWS region for the state bucket and lock table"
  type        = string
  default     = "eu-west-1"
}

variable "project_name" {
  description = "Name prefix for the state bucket and lock table"
  type        = string
  default     = "ecommerce-lakehouse"
}

provider "aws" {
  region = var.aws_region
}

resource "random_id" "state_bucket_suffix" {
  byte_length = 4
}

resource "aws_s3_bucket" "tf_state" {
  bucket = "${var.project_name}-tfstate-${random_id.state_bucket_suffix.hex}"

  # Deliberately no force_destroy: losing this bucket loses the state file
  # for every other environment/resource this project manages.
}

resource "aws_s3_bucket_versioning" "tf_state" {
  bucket = aws_s3_bucket.tf_state.id
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "tf_state" {
  bucket = aws_s3_bucket.tf_state.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_s3_bucket_public_access_block" "tf_state" {
  bucket                  = aws_s3_bucket.tf_state.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_dynamodb_table" "tf_lock" {
  name         = "${var.project_name}-tfstate-lock"
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "LockID"

  attribute {
    name = "LockID"
    type = "S"
  }
}

output "state_bucket_name" {
  value = aws_s3_bucket.tf_state.id
}

output "lock_table_name" {
  value = aws_dynamodb_table.tf_lock.name
}
