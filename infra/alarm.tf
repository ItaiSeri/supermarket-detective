# set in infra/terraform.tfvars, which is git-ignored: this repo is public
variable "alarm_email" {
  description = "Address that receives pipeline failure emails"
  type        = string
  sensitive   = true
}

# the mailing list
resource "aws_sns_topic" "alarms" {
  name = "supermarket-detective-alarms"
}

# one address on the list; stays pending until the confirmation email is clicked
resource "aws_sns_topic_subscription" "alarm_email" {
  topic_arn = aws_sns_topic.alarms.arn
  protocol  = "email"
  endpoint  = var.alarm_email
}

# the listener: a run of our state machine ended badly
resource "aws_cloudwatch_event_rule" "collect_failed" {
  name        = "supermarket-detective-collect-failed"
  description = "The daily collection run failed, timed out or was aborted"

  event_pattern = jsonencode({
    source        = ["aws.states"]
    "detail-type" = ["Step Functions Execution Status Change"]
    detail = {
      stateMachineArn = [aws_sfn_state_machine.collect.arn]
      status          = ["FAILED", "TIMED_OUT", "ABORTED"]
    }
  })
}

# what the listener does: post a short readable message to the mailing list
resource "aws_cloudwatch_event_target" "collect_failed_email" {
  rule = aws_cloudwatch_event_rule.collect_failed.name
  arn  = aws_sns_topic.alarms.arn

  input_transformer {
    input_paths = {
      status = "$.detail.status"
      run    = "$.detail.name"
    }
    input_template = "\"Supermarket Detective: run <run> ended as <status>. Open Step Functions in il-central-1 to see which items failed.\""
  }
}

# only this one rule may post to the mailing list
data "aws_iam_policy_document" "alarms_topic" {
  statement {
    actions   = ["sns:Publish"]
    resources = [aws_sns_topic.alarms.arn]
    principals {
      type        = "Service"
      identifiers = ["events.amazonaws.com"]
    }
    condition {
      test     = "ArnEquals"
      variable = "aws:SourceArn"
      values   = [aws_cloudwatch_event_rule.collect_failed.arn]
    }
  }
}

resource "aws_sns_topic_policy" "alarms" {
  arn    = aws_sns_topic.alarms.arn
  policy = data.aws_iam_policy_document.alarms_topic.json
}
