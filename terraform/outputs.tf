output "bucket_name" { value = aws_s3_bucket.project.id }
output "database_name" { value = aws_glue_catalog_database.analytics.name }
output "athena_workgroup" { value = aws_athena_workgroup.project.name }
output "ingest_job_name" { value = aws_glue_job.ingest.name }
output "etl_job_name" { value = aws_glue_job.etl.name }
output "archive_day" { value = var.archive_day }
output "aws_region" { value = var.aws_region }
