# Raw bucket for the Kaggle backfill, in Ohio next to Databricks (the scraper bucket stays in Tel Aviv).
# Layout: source=kaggle/date=<version date>/chain=<CHAIN>/<name>.gz and source=kaggle/_manifest/...
resource "aws_s3_bucket" "kaggle" {
  provider = aws.ohio
  bucket   = "supermarket-detective-kaggle-${data.aws_caller_identity.current.account_id}"
}

resource "aws_s3_bucket_versioning" "kaggle" {
  provider = aws.ohio
  bucket   = aws_s3_bucket.kaggle.id

  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_public_access_block" "kaggle" {
  provider = aws.ohio
  bucket   = aws_s3_bucket.kaggle.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_lifecycle_configuration" "kaggle" {
  provider = aws.ohio
  bucket   = aws_s3_bucket.kaggle.id

  rule {
    id     = "intelligent-tiering"
    status = "Enabled"

    filter {}

    transition {
      days          = 0
      storage_class = "INTELLIGENT_TIERING"
    }

    noncurrent_version_expiration {
      noncurrent_days = 30
    }

    abort_incomplete_multipart_upload {
      days_after_initiation = 7
    }
  }

  depends_on = [aws_s3_bucket_versioning.kaggle]
}

output "kaggle_bucket" {
  value = aws_s3_bucket.kaggle.bucket
}
