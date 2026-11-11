# When the collection finishes, Step Functions starts the Databricks daily job
# (bronze_load, then silver_load) through the Jobs API.

# set both in infra/terraform.tfvars, which is git-ignored: this repo is public
variable "databricks_token" {
  description = "Databricks personal access token used to start the daily job"
  type        = string
  sensitive   = true
}

variable "databricks_daily_job_id" {
  description = "Job ID of the Databricks parent job (bronze_load then silver_load)"
  type        = number
}

locals {
  databricks_host        = "https://dbc-ac0d914f-528b.cloud.databricks.com"
  databricks_run_now_url = "${local.databricks_host}/api/2.2/jobs/run-now"
}

# the saved login: EventBridge keeps the token in its own Secrets Manager secret
# and adds the Authorization header to every call made with this connection
resource "aws_cloudwatch_event_connection" "databricks" {
  name               = "supermarket-detective-databricks"
  description        = "Databricks Jobs API login for the daily trigger"
  authorization_type = "API_KEY"

  auth_parameters {
    api_key {
      key   = "Authorization"
      value = "Bearer ${var.databricks_token}"
    }
  }
}

# extra rights for the state machine role: call this one URL with POST, using
# only this connection and the secret behind it
data "aws_iam_policy_document" "collect_databricks" {
  statement {
    actions   = ["states:InvokeHTTPEndpoint"]
    resources = [aws_sfn_state_machine.collect.arn]
    condition {
      test     = "StringEquals"
      variable = "states:HTTPMethod"
      values   = ["POST"]
    }
    condition {
      test     = "StringLike"
      variable = "states:HTTPEndpoint"
      values   = [local.databricks_run_now_url]
    }
  }

  statement {
    actions   = ["events:RetrieveConnectionCredentials"]
    resources = [aws_cloudwatch_event_connection.databricks.arn]
  }

  statement {
    actions   = ["secretsmanager:GetSecretValue", "secretsmanager:DescribeSecret"]
    resources = [aws_cloudwatch_event_connection.databricks.secret_arn]
  }
}

resource "aws_iam_role_policy" "collect_databricks" {
  name   = "start-databricks-job"
  role   = aws_iam_role.collect.id
  policy = data.aws_iam_policy_document.collect_databricks.json
}
