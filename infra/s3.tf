data "aws_caller_identity" "current" {}

resource "aws_s3_bucket" "raw" {
  bucket = "supermarket-detective-raw-${data.aws_caller_identity.current.account_id}"
}

resource "aws_s3_bucket_versioning" "raw" {
  bucket = aws_s3_bucket.raw.id

  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_public_access_block" "raw" {
  bucket = aws_s3_bucket.raw.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# raw files are read about once (into bronze), then kept as an archive:
# Intelligent-Tiering moves each file to cheaper tiers by itself when nobody reads it
resource "aws_s3_bucket_lifecycle_configuration" "raw" {
  bucket = aws_s3_bucket.raw.id

  rule {
    id     = "intelligent-tiering"
    status = "Enabled"

    filter {} # whole bucket

    transition {
      days          = 0
      storage_class = "INTELLIGENT_TIERING"
    }

    # versioning keeps overwritten files; drop those old copies after 30 days
    noncurrent_version_expiration {
      noncurrent_days = 30
    }

    # clean up the parts of uploads that failed midway (invisible, but billed)
    abort_incomplete_multipart_upload {
      days_after_initiation = 7
    }
  }

  # rules that touch old versions need versioning to exist first
  depends_on = [aws_s3_bucket_versioning.raw]
}