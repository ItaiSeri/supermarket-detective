variable "databricks_external_id" {
  description = "Databricks account ID, proves the request comes from our account"
  type        = string
  sensitive   = true
}

locals {
  databricks_role_name = "supermarket-detective-databricks-read"
  account_id           = data.aws_caller_identity.current.account_id
}

# who may use this role: Databricks' Unity Catalog, and the role itself
data "aws_iam_policy_document" "databricks_trust" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "AWS"
      identifiers = ["arn:aws:iam::414351767826:role/unity-catalog-prod-UCMasterRole-14S5ZJVKOTYTL"]
    }
    condition {
      test     = "StringEquals"
      variable = "sts:ExternalId"
      values   = [var.databricks_external_id]
    }
  }

  # Unity Catalog requires the role to be able to assume itself
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "AWS"
      identifiers = ["arn:aws:iam::${local.account_id}:root"]
    }
    condition {
      test     = "ArnLike"
      variable = "aws:PrincipalArn"
      values   = ["arn:aws:iam::${local.account_id}:role/${local.databricks_role_name}"]
    }
  }
}

resource "aws_iam_role" "databricks_read" {
  name               = local.databricks_role_name
  assume_role_policy = data.aws_iam_policy_document.databricks_trust.json
}

# what this role may do: read the raw bucket, nothing else
data "aws_iam_policy_document" "databricks_read" {
  statement {
    actions   = ["s3:ListBucket", "s3:GetBucketLocation"]
    resources = [aws_s3_bucket.raw.arn]
  }
  statement {
    actions   = ["s3:GetObject", "s3:GetObjectVersion"]
    resources = ["${aws_s3_bucket.raw.arn}/*"]
  }
  # Unity Catalog checks that the role can assume itself
  statement {
    actions   = ["sts:AssumeRole"]
    resources = ["arn:aws:iam::${local.account_id}:role/${local.databricks_role_name}"]
  }
}

resource "aws_iam_role_policy" "databricks_read" {
  name   = "read-raw-bucket"
  role   = aws_iam_role.databricks_read.id
  policy = data.aws_iam_policy_document.databricks_read.json
}