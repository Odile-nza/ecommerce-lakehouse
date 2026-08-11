resource "aws_cloudwatch_event_rule" "trigger_lakehouse_etl" {
  name = "${var.project_name}-trigger"

  event_pattern = jsonencode({
    source      = ["aws.s3"]
    detail-type = ["Object Created"]
    detail = {
      bucket = { name = [aws_s3_bucket.this.id] }
      object = {
        key = [
          { prefix = "raw/products/" },
          { prefix = "raw/orders/" },
          { prefix = "raw/order_items/" },
        ]
      }
    }
  })

  depends_on = [aws_s3_bucket_notification.eventbridge]
}

resource "aws_cloudwatch_event_target" "state_machine" {
  rule     = aws_cloudwatch_event_rule.trigger_lakehouse_etl.name
  arn      = aws_sfn_state_machine.this.arn
  role_arn = aws_iam_role.eventbridge_invoke_state_machine.arn

  input_transformer {
    input_paths = {
      bucket = "$.detail.bucket.name"
      key    = "$.detail.object.key"
    }
    input_template = "{\"bucket\":<bucket>,\"key\":<key>}"
  }
}
