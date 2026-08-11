variable "aws_region" {
  description = "AWS region to deploy into"
  type        = string
  default     = "eu-west-1"
}

variable "project_name" {
  description = "Name prefix applied to the state machine, EventBridge rule and resource tags"
  type        = string
  default     = "ecommerce-lakehouse"
}

variable "bucket_name_prefix" {
  description = "Prefix for the S3 bucket name; a random suffix is appended for global uniqueness"
  type        = string
  default     = "ecommerce-lakehouse"
}

variable "glue_database_name" {
  description = "Glue Data Catalog database that holds the products/orders/order_items Delta tables"
  type        = string
  default     = "ecommerce_lakehouse"
}

variable "alert_email" {
  description = "Email address subscribed to pipeline failure alerts"
  type        = string
}
