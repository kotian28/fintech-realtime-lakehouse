output "buckets" { value = { for k, b in aws_s3_bucket.layer : k => b.bucket } }
output "pipeline_role_arn" { value = aws_iam_role.pipeline.arn }
output "athena_workgroup" { value = aws_athena_workgroup.main.name }
output "glue_database" { value = aws_glue_catalog_database.lake.name }
