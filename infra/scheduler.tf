# who may use this role: only EventBridge Scheduler
data "aws_iam_policy_document" "scheduler_assume" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["scheduler.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "daily_schedule" {
  name               = "supermarket-detective-daily-schedule"
  assume_role_policy = data.aws_iam_policy_document.scheduler_assume.json
}

# the only thing this role can do: start our one state machine
data "aws_iam_policy_document" "daily_schedule_start" {
  statement {
    actions   = ["states:StartExecution"]
    resources = [aws_sfn_state_machine.collect.arn]
  }
}

resource "aws_iam_role_policy" "daily_schedule_start" {
  name   = "start-collect"
  role   = aws_iam_role.daily_schedule.id
  policy = data.aws_iam_policy_document.daily_schedule_start.json
}

resource "aws_scheduler_schedule" "daily_collect" {
  name = "supermarket-detective-daily-collect"
  # 09:00, because the scraper disables Victory and Mahsani Ashuk until 08:00
  schedule_expression          = "cron(0 9 * * ? *)"
  schedule_expression_timezone = "Asia/Jerusalem"

  flexible_time_window {
    mode = "OFF"
  }

  target {
    arn      = aws_sfn_state_machine.collect.arn
    role_arn = aws_iam_role.daily_schedule.arn
    input    = jsonencode({ items = local.work_list })
  }
}