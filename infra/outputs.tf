output "raw_bucket" {
  value = aws_s3_bucket.raw.bucket
}

output "ecr_repository_url" {
  value = aws_ecr_repository.fetch_chain.repository_url
}
output "lambda_name" {
  value = aws_lambda_function.fetch_chain.function_name
}
output "collect_state_machine_arn" {
  value = aws_sfn_state_machine.collect.arn
}

output "work_list" {
  value = local.work_list
}
output "databricks_connection_arn" {
  value = aws_cloudwatch_event_connection.databricks.arn
}
