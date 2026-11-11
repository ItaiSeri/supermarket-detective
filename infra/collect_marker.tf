# When the collection ends, Step Functions writes one small marker file to S3.
# Databricks job daily_load has a file arrival trigger on that folder, so the
# marker starts bronze_load then silver_load.
#
# Why a file and not an API call: EventBridge Connections (needed for the Step
# Functions HTTP task) are disabled in il-central-1. A Lambda holding a Databricks
# token was rejected (the token expires every 90 days), and a relay in another
# region was rejected as extra moving parts. This way AWS stores no Databricks
# token at all.

# the only thing the state machine may write for this: marker files in their own folder
data "aws_iam_policy_document" "collect_marker" {
  statement {
    actions   = ["s3:PutObject"]
    resources = ["${aws_s3_bucket.raw.arn}/source=scraper/_markers/*"]
  }
}

resource "aws_iam_role_policy" "collect_marker" {
  name   = "write-collect-marker"
  role   = aws_iam_role.collect.id
  policy = data.aws_iam_policy_document.collect_marker.json
}
