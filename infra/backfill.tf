# Kaggle history backfill (milestone 3): a small zip Lambda that copies one Kaggle file
# into the raw bucket, and a state machine that runs it over a slice of files. The state
# machine is started by hand (no schedule) and writes no marker, so it never starts daily_load.

# the code is one file using only the standard library and boto3 (already in the runtime)
data "archive_file" "fetch_kaggle" {
  type        = "zip"
  source_file = "${path.module}/../ingestion/fetch_kaggle/handler.py"
  output_path = "${path.module}/build/fetch_kaggle.zip"
}

resource "aws_iam_role" "fetch_kaggle" {
  name               = "supermarket-detective-fetch-kaggle"
  assume_role_policy = data.aws_iam_policy_document.lambda_assume.json # same trust as fetch_chain: the Lambda service
}

resource "aws_iam_role_policy_attachment" "fetch_kaggle_logs" {
  role       = aws_iam_role.fetch_kaggle.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
}

# upload under source=kaggle/ only; read back only the manifests (to skip what is already copied)
data "aws_iam_policy_document" "fetch_kaggle_s3" {
  statement {
    actions   = ["s3:PutObject", "s3:AbortMultipartUpload"]
    resources = ["${aws_s3_bucket.raw.arn}/source=kaggle/*"]
  }
  statement {
    actions   = ["s3:GetObject"]
    resources = ["${aws_s3_bucket.raw.arn}/source=kaggle/*"]
  }
}

resource "aws_iam_role_policy" "fetch_kaggle_s3" {
  name   = "write-raw-kaggle"
  role   = aws_iam_role.fetch_kaggle.id
  policy = data.aws_iam_policy_document.fetch_kaggle_s3.json
}

resource "aws_cloudwatch_log_group" "fetch_kaggle" {
  name              = "/aws/lambda/supermarket-detective-fetch-kaggle"
  retention_in_days = 30
}

resource "aws_lambda_function" "fetch_kaggle" {
  function_name    = "supermarket-detective-fetch-kaggle"
  role             = aws_iam_role.fetch_kaggle.arn
  runtime          = "python3.12"
  handler          = "handler.handler"
  filename         = data.archive_file.fetch_kaggle.output_path
  source_code_hash = data.archive_file.fetch_kaggle.output_base64sha256
  architectures    = ["x86_64"]
  timeout          = 900
  memory_size      = 1024

  # the file is gzipped while it streams in, so /tmp holds only the small .gz
  ephemeral_storage {
    size = 4096
  }

  environment {
    variables = {
      RAW_BUCKET = aws_s3_bucket.raw.bucket
    }
  }

  depends_on = [
    aws_iam_role_policy_attachment.fetch_kaggle_logs,
    aws_cloudwatch_log_group.fetch_kaggle,
  ]
}

# who may use this role: Step Functions; it may only run the one Lambda above
resource "aws_iam_role" "backfill" {
  name               = "supermarket-detective-backfill"
  assume_role_policy = data.aws_iam_policy_document.sfn_assume.json
}

data "aws_iam_policy_document" "backfill_invoke" {
  statement {
    actions   = ["lambda:InvokeFunction"]
    resources = [aws_lambda_function.fetch_kaggle.arn]
  }
}

resource "aws_iam_role_policy" "backfill_invoke" {
  name   = "invoke-fetch-kaggle"
  role   = aws_iam_role.backfill.id
  policy = data.aws_iam_policy_document.backfill_invoke.json
}

resource "aws_sfn_state_machine" "backfill" {
  name     = "supermarket-detective-backfill"
  role_arn = aws_iam_role.backfill.arn

  definition = jsonencode({
    QueryLanguage = "JSONata"
    StartAt       = "CopyAll"
    States = {
      CopyAll = {
        Type           = "Map"
        Items          = "{% $states.input.items %}"
        MaxConcurrency = 5
        ItemProcessor = {
          ProcessorConfig = { Mode = "INLINE" }
          StartAt         = "CopyFile"
          States = {
            CopyFile = {
              Type     = "Task"
              Resource = "arn:aws:states:::lambda:invoke"
              Arguments = {
                FunctionName = aws_lambda_function.fetch_kaggle.arn
                Payload      = "{% $states.input %}"
              }
              # keep only the short facts of each copy, not the whole payload
              Output = "{% {'status': $states.result.Payload.status, 'version': $states.result.Payload.version, 'chain': $states.result.Payload.chain, 'kind': $states.result.Payload.kind, 'kaggle_listed_bytes': $states.result.Payload.kaggle_listed_bytes, 's3_bytes': $states.result.Payload.s3_bytes, 'secs': $states.result.Payload.secs, 'skipped': $states.result.Payload.skipped} %}"
              # Kaggle may rate-limit or drop a download: wait 1, then 2, then 4 minutes
              Retry = [{
                ErrorEquals     = ["States.ALL"]
                IntervalSeconds = 60
                MaxAttempts     = 3
                BackoffRate     = 2
              }]
              Catch = [{
                ErrorEquals = ["States.ALL"]
                Output      = "{% $merge([$states.input, {'status': 'failed', 'error': $states.errorOutput.Error}]) %}"
                Next        = "ItemFailed"
              }]
              End = true
            }
            ItemFailed = { Type = "Pass", End = true }
          }
        }
        Assign = {
          failed = "{% $count($states.result[status = 'failed']) %}"
        }
        Output = "{% {'copied': $count($states.result[status = 'ok']), 'failed': $count($states.result[status = 'failed']), 'items': $states.result} %}"
        Next   = "CheckResults"
      }
      CheckResults = {
        Type = "Choice"
        Choices = [{
          Condition = "{% $failed > 0 %}"
          Next      = "SomeFailed"
        }]
        Default = "AllOk"
      }
      SomeFailed = {
        Type  = "Fail"
        Error = "SomeFilesFailed"
        Cause = "At least one Kaggle file failed after its retries; see the Map results. Rerunning the same slice skips files that already have a manifest."
      }
      AllOk = { Type = "Succeed" }
    }
  })
}

output "backfill_state_machine_arn" {
  value = aws_sfn_state_machine.backfill.arn
}

output "fetch_kaggle_lambda_name" {
  value = aws_lambda_function.fetch_kaggle.function_name
}
