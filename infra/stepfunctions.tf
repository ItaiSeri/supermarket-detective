locals {
  chains     = jsondecode(file("${path.module}/chains.json"))
  file_types = ["STORE_FILE", "PRICE_FULL_FILE", "PROMO_FULL_FILE"]

  # one input per chain + file type: 10 x 3 = 30
  work_list = flatten([
    for chain in local.chains : [
      for file_type in local.file_types : {
        chain      = chain
        file_types = [file_type]
      }
    ]
  ])
}

# who may use this role: only Step Functions
data "aws_iam_policy_document" "sfn_assume" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["states.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "collect" {
  name               = "supermarket-detective-collect"
  assume_role_policy = data.aws_iam_policy_document.sfn_assume.json
}

# the only thing this role can do: run our one Lambda
data "aws_iam_policy_document" "collect_invoke" {
  statement {
    actions   = ["lambda:InvokeFunction"]
    resources = [aws_lambda_function.fetch_chain.arn]
  }
}

resource "aws_iam_role_policy" "collect_invoke" {
  name   = "invoke-fetch-chain"
  role   = aws_iam_role.collect.id
  policy = data.aws_iam_policy_document.collect_invoke.json
}

resource "aws_sfn_state_machine" "collect" {
  name     = "supermarket-detective-collect"
  role_arn = aws_iam_role.collect.arn

  definition = jsonencode({
    QueryLanguage = "JSONata"
    StartAt       = "CollectAll"
    States = {
      CollectAll = {
        Type           = "Map"
        Items          = "{% $states.input.items %}"
        MaxConcurrency = 10
        ItemProcessor = {
          ProcessorConfig = { Mode = "INLINE" }
          StartAt         = "FetchFiles"
          States = {
            FetchFiles = {
              Type     = "Task"
              Resource = "arn:aws:states:::lambda:invoke"
              Arguments = {
                FunctionName = aws_lambda_function.fetch_chain.arn
                Payload      = "{% $states.input %}"
              }
              Output = "{% $states.result.Payload %}"
              # a chain site may be down for a while: wait 10, then 20 minutes
              Retry = [{
                ErrorEquals     = ["States.ALL"]
                IntervalSeconds = 600
                MaxAttempts     = 2
                BackoffRate     = 2
              }]
              # after the retries, record the failure and let the other items finish
              Catch = [{
                ErrorEquals = ["States.ALL"]
                Output      = "{% $merge([$states.input, {'ok': false, 'error': $states.errorOutput.Error}]) %}"
                Next        = "ItemFailed"
              }]
              End = true
            }
            ItemFailed = { Type = "Pass", End = true }
          }
        }
        # remember whether any item failed; Databricks starts either way
        Assign = {
          someFailed = "{% $count($states.result[ok = false]) > 0 %}"
        }
        # if the Map itself breaks, also go on to Databricks and fail at the end
        Catch = [{
          ErrorEquals = ["States.ALL"]
          Assign      = { someFailed = true }
          Next        = "StartDatabricks"
        }]
        Next = "StartDatabricks"
      }
      # start the Databricks parent job (bronze_load then silver_load); see databricks_trigger.tf
      StartDatabricks = {
        Type     = "Task"
        Resource = "arn:aws:states:::http:invoke"
        Arguments = {
          ApiEndpoint    = local.databricks_run_now_url
          Method         = "POST"
          Authentication = { ConnectionArn = aws_cloudwatch_event_connection.databricks.arn }
          RequestBody    = { job_id = var.databricks_daily_job_id }
        }
        # a failed call after the retries fails the whole run, so the alarm email fires
        Retry = [{
          ErrorEquals     = ["States.ALL"]
          IntervalSeconds = 30
          MaxAttempts     = 3
          BackoffRate     = 2
        }]
        Next = "CheckResults"
      }
      # Databricks is started: mark the whole run failed if any item failed
      CheckResults = {
        Type = "Choice"
        Choices = [{
          Condition = "{% $someFailed %}"
          Next      = "SomeFailed"
        }]
        Default = "AllOk"
      }
      SomeFailed = {
        Type  = "Fail"
        Error = "SomeItemsFailed"
        Cause = "At least one chain + file type failed (Databricks was started anyway); see the Map results"
      }
      AllOk = { Type = "Succeed" }
    }
  })
}