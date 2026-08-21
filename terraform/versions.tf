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
    archive = {
      source  = "hashicorp/archive"
      version = "~> 2.4"
    }
  }

  # Partial backend config: bucket/key/region/dynamodb_table are supplied at
  # `terraform init -backend-config=backend.hcl` time (see backend.hcl.example),
  # not hardcoded here, so this file doesn't bake in one account's bucket name.
  # Provisioned once via terraform/bootstrap/ — see that module's comments.
  backend "s3" {}
}

provider "aws" {
  region = var.aws_region

  default_tags {
    tags = {
      Project = var.project_name
    }
  }
}
