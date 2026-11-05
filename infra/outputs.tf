output "raw_bucket" {
  value = aws_s3_bucket.raw.bucket
}

output "ecr_repository_url" {
  value = aws_ecr_repository.fetch_chain.repository_url
}
output "lambda_name" {
  value = aws_lambda_function.fetch_chain.function_name
}