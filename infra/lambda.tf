variable "fetch_chain_image_tag" {
  description = "Which image version in ECR the Lambda runs"
  type        = string
  default     = "v2"
}

# who may use this role: only the Lambda service
data "aws_iam_policy_document" "lambda_assume" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["lambda.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "fetch_chain" {
  name               = "supermarket-detective-fetch-chain"
  assume_role_policy = data.aws_iam_policy_document.lambda_assume.json
}

# AWS-managed policy: write logs to CloudWatch
resource "aws_iam_role_policy_attachment" "fetch_chain_logs" {
  role       = aws_iam_role.fetch_chain.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
}

# our own policy: upload files under source=scraper/ only
data "aws_iam_policy_document" "fetch_chain_s3" {
  statement {
    actions   = ["s3:PutObject", "s3:AbortMultipartUpload"]
    resources = ["${aws_s3_bucket.raw.arn}/source=scraper/*"]
  }
}

resource "aws_iam_role_policy" "fetch_chain_s3" {
  name   = "write-raw-scraper"
  role   = aws_iam_role.fetch_chain.id
  policy = data.aws_iam_policy_document.fetch_chain_s3.json
}

resource "aws_cloudwatch_log_group" "fetch_chain" {
  name              = "/aws/lambda/supermarket-detective-fetch-chain"
  retention_in_days = 30
}

resource "aws_lambda_function" "fetch_chain" {
  function_name = "supermarket-detective-fetch-chain"
  role          = aws_iam_role.fetch_chain.arn
  package_type  = "Image"
  image_uri     = "${aws_ecr_repository.fetch_chain.repository_url}:${var.fetch_chain_image_tag}"
  architectures = ["x86_64"]
  timeout       = 900
  memory_size   = 1024

  ephemeral_storage {
    size = 2048
  }

  environment {
    variables = {
      RAW_BUCKET = aws_s3_bucket.raw.bucket
    }
  }

  depends_on = [
    aws_iam_role_policy_attachment.fetch_chain_logs,
    aws_cloudwatch_log_group.fetch_chain,
  ]
}