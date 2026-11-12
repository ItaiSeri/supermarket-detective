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
    resources = ["${aws_s3_bucket.kaggle.arn}/source=kaggle/*"]
  }
  statement {
    actions   = ["s3:GetObject"]
    resources = ["${aws_s3_bucket.kaggle.arn}/source=kaggle/*"]
  }
}

resource "aws_iam_role_policy" "fetch_kaggle_s3" {
  name   = "write-raw-kaggle"
  role   = aws_iam_role.fetch_kaggle.id
  policy = data.aws_iam_policy_document.fetch_kaggle_s3.json
}

resource "aws_cloudwatch_log_group" "fetch_kaggle" {
  provider          = aws.ohio
  name              = "/aws/lambda/supermarket-detective-fetch-kaggle"
  retention_in_days = 30
}

resource "aws_lambda_function" "fetch_kaggle" {
  provider         = aws.ohio
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
      RAW_BUCKET = aws_s3_bucket.kaggle.bucket
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

# role extras for the Distributed Map: read the item list, write the result files, start its own child executions
data "aws_iam_policy_document" "backfill_map" {
  statement {
    actions   = ["s3:GetObject"]
    resources = ["${aws_s3_bucket.kaggle.arn}/_backfill/*"]
  }
  statement {
    actions   = ["s3:ListBucket"]
    resources = [aws_s3_bucket.kaggle.arn]
  }
  statement {
    actions   = ["s3:PutObject", "s3:AbortMultipartUpload"]
    resources = ["${aws_s3_bucket.kaggle.arn}/_backfill_results/*"]
  }
  statement {
    actions   = ["states:StartExecution"]
    resources = ["arn:aws:states:us-east-2:${data.aws_caller_identity.current.account_id}:stateMachine:supermarket-detective-backfill"]
  }
  statement {
    actions   = ["states:DescribeExecution", "states:StopExecution"]
    resources = ["arn:aws:states:us-east-2:${data.aws_caller_identity.current.account_id}:execution:supermarket-detective-backfill/*"]
  }
}

resource "aws_iam_role_policy" "backfill_map" {
  name   = "distributed-map"
  role   = aws_iam_role.backfill.id
  policy = data.aws_iam_policy_document.backfill_map.json
}

# Input: {"items_key": "_backfill/items.json"}  (a JSON array of items in the Kaggle bucket)
resource "aws_sfn_state_machine" "backfill" {
  provider = aws.ohio
  name     = "supermarket-detective-backfill"
  role_arn = aws_iam_role.backfill.arn

  definition = jsonencode({
    QueryLanguage = "JSONata"
    StartAt       = "CopyAll"
    States = {
      CopyAll = {
        Type = "Map"
        ItemReader = {
          Resource = "arn:aws:states:::s3:getObject"
          ReaderConfig = {
            InputType = "JSON"
          }
          Arguments = {
            Bucket = aws_s3_bucket.kaggle.bucket
            Key    = "{% $states.input.items_key %}"
          }
        }
        MaxConcurrency             = 10
        ToleratedFailurePercentage = 100 # failed items are listed in the results; the end state reports them
        ItemProcessor = {
          ProcessorConfig = { Mode = "DISTRIBUTED", ExecutionType = "STANDARD" }
          StartAt         = "CopyFile"
          States = {
            CopyFile = {
              Type     = "Task"
              Resource = "arn:aws:states:::lambda:invoke"
              Arguments = {
                FunctionName = aws_lambda_function.fetch_kaggle.arn
                Payload      = "{% $states.input %}"
              }
              Output = "{% {'status': $states.result.Payload.status, 'version': $states.result.Payload.version, 'chain': $states.result.Payload.chain, 'kind': $states.result.Payload.kind, 's3_bytes': $states.result.Payload.s3_bytes, 'skipped': $states.result.Payload.skipped} %}"
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
        ResultWriter = {
          Resource = "arn:aws:states:::s3:putObject"
          Arguments = {
            Bucket = aws_s3_bucket.kaggle.bucket
            Prefix = "_backfill_results"
          }
        }
        End = true
      }
    }
  })
}

output "backfill_state_machine_arn" {
  value = aws_sfn_state_machine.backfill.arn
}

output "fetch_kaggle_lambda_name" {
  value = aws_lambda_function.fetch_kaggle.function_name
}
