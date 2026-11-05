output "raw_bucket" {
  value = aws_s3_bucket.raw.bucket
}

output "ecr_repository_url" {
  value = aws_ecr_repository.fetch_chain.repository_url
}
