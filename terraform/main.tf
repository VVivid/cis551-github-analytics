resource "aws_s3_bucket" "project" {
  bucket        = local.bucket
  force_destroy = var.allow_bucket_destroy
}

resource "aws_s3_bucket_public_access_block" "project" {
  bucket                  = aws_s3_bucket.project.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_server_side_encryption_configuration" "project" {
  bucket = aws_s3_bucket.project.id
  rule {
    apply_server_side_encryption_by_default { sse_algorithm = "AES256" }
  }
}

resource "aws_s3_bucket_policy" "tls" {
  bucket = aws_s3_bucket.project.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Sid       = "DenyInsecureTransport", Effect = "Deny", Principal = "*", Action = "s3:*"
      Resource  = [aws_s3_bucket.project.arn, "${aws_s3_bucket.project.arn}/*"]
      Condition = { Bool = { "aws:SecureTransport" = "false" } }
    }]
  })
}

resource "aws_s3_bucket_lifecycle_configuration" "temporary" {
  bucket = aws_s3_bucket.project.id
  rule {
    id     = "expire-query-results"
    status = "Enabled"
    filter { prefix = "athena-results/" }
    expiration { days = 7 }
    abort_incomplete_multipart_upload { days_after_initiation = 1 }
  }
  rule {
    id     = "expire-spark-temp"
    status = "Enabled"
    filter { prefix = "temporary/" }
    expiration { days = 2 }
    abort_incomplete_multipart_upload { days_after_initiation = 1 }
  }
}

resource "aws_s3_object" "scripts" {
  for_each = toset(["ingest.py", "etl.py"])
  bucket   = aws_s3_bucket.project.id
  key      = "scripts/${each.value}"
  source   = "${path.module}/../jobs/${each.value}"
  etag     = filemd5("${path.module}/../jobs/${each.value}")
}

resource "aws_iam_role" "glue" {
  name = "${var.project_name}-glue"
  assume_role_policy = jsonencode({
    Version   = "2012-10-17"
    Statement = [{ Effect = "Allow", Principal = { Service = "glue.amazonaws.com" }, Action = "sts:AssumeRole" }]
  })
}

resource "aws_cloudwatch_log_group" "glue" {
  for_each          = toset(["output", "error"])
  name              = "/${var.project_name}/${each.value}"
  retention_in_days = 14
}

resource "aws_iam_role_policy" "glue" {
  role = aws_iam_role.glue.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      { Effect = "Allow", Action = ["s3:GetBucketLocation", "s3:ListBucket", "s3:ListBucketMultipartUploads"], Resource = aws_s3_bucket.project.arn },
      { Effect = "Allow", Action = ["s3:GetObject"], Resource = ["${aws_s3_bucket.project.arn}/scripts/*", "${aws_s3_bucket.project.arn}/raw/*"] },
      { Effect = "Allow", Action = ["s3:GetObject", "s3:PutObject", "s3:DeleteObject", "s3:AbortMultipartUpload", "s3:ListMultipartUploadParts"], Resource = ["${aws_s3_bucket.project.arn}/raw/*", "${aws_s3_bucket.project.arn}/curated/*", "${aws_s3_bucket.project.arn}/temporary/*"] },
      { Effect = "Allow", Action = ["logs:CreateLogGroup", "logs:CreateLogStream", "logs:PutLogEvents"], Resource = ["arn:${local.partition}:logs:${var.aws_region}:${local.account_id}:log-group:/${var.project_name}/*", "arn:${local.partition}:logs:${var.aws_region}:${local.account_id}:log-group:/aws-glue/python-jobs/*"] }
    ]
  })
}

resource "aws_glue_job" "ingest" {
  name         = "${var.project_name}-ingest"
  role_arn     = aws_iam_role.glue.arn
  max_capacity = 0.0625
  timeout      = 45
  max_retries  = 0
  command {
    name            = "pythonshell"
    python_version  = "3.9"
    script_location = "s3://${aws_s3_bucket.project.id}/${aws_s3_object.scripts["ingest.py"].key}"
  }
  execution_property { max_concurrent_runs = 1 }
  default_arguments = {
    "--BUCKET"      = aws_s3_bucket.project.id
    "--DAY"         = var.archive_day
    "--HOURS"       = "all"
    "--library-set" = "analytics"
  }
  depends_on = [aws_iam_role_policy.glue]
}

resource "aws_glue_job" "etl" {
  name              = "${var.project_name}-etl"
  role_arn          = aws_iam_role.glue.arn
  glue_version      = "5.0"
  worker_type       = "G.1X"
  number_of_workers = 2
  timeout           = 30
  max_retries       = 0
  command {
    name            = "glueetl"
    python_version  = "3"
    script_location = "s3://${aws_s3_bucket.project.id}/${aws_s3_object.scripts["etl.py"].key}"
  }
  execution_property { max_concurrent_runs = 1 }
  default_arguments = {
    "--BUCKET"                       = aws_s3_bucket.project.id
    "--DAY"                          = var.archive_day
    "--HOURS"                        = "all"
    "--TempDir"                      = "s3://${aws_s3_bucket.project.id}/temporary/"
    "--job-bookmark-option"          = "job-bookmark-disable"
    "--enable-metrics"               = "false"
    "--enable-observability-metrics" = "false"
    "--custom-logGroup-prefix"       = "/${var.project_name}"
  }
  depends_on = [aws_iam_role_policy.glue]
}

resource "aws_glue_catalog_database" "analytics" {
  name = local.database
}

locals {
  event_columns      = { event_id = "string", repository = "string", event_created_at = "string", archive_day = "string", archive_hour = "int", batch_id = "string" }
  completion_columns = { archive_day = "string", archive_hour = "int", batch_id = "string", raw_events = "bigint", opening_candidates = "bigint", rejected_openings = "bigint", duplicate_openings = "bigint", opening_count = "bigint", completed_at = "string" }
}

resource "aws_glue_catalog_table" "events" {
  name          = "opening_events"
  database_name = aws_glue_catalog_database.analytics.name
  table_type    = "EXTERNAL_TABLE"
  parameters    = { classification = "parquet", EXTERNAL = "TRUE" }
  storage_descriptor {
    location      = "s3://${aws_s3_bucket.project.id}/curated/events/"
    input_format  = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat"
    output_format = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat"
    ser_de_info { serialization_library = "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe" }
    dynamic "columns" {
      for_each = local.event_columns
      content {
        name = columns.key
        type = columns.value
      }
    }
  }
}

resource "aws_glue_catalog_table" "completions" {
  name          = "hour_completions"
  database_name = aws_glue_catalog_database.analytics.name
  table_type    = "EXTERNAL_TABLE"
  parameters    = { classification = "json", EXTERNAL = "TRUE" }
  storage_descriptor {
    location      = "s3://${aws_s3_bucket.project.id}/curated/completions/"
    input_format  = "org.apache.hadoop.mapred.TextInputFormat"
    output_format = "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat"
    ser_de_info { serialization_library = "org.openx.data.jsonserde.JsonSerDe" }
    dynamic "columns" {
      for_each = local.completion_columns
      content {
        name = columns.key
        type = columns.value
      }
    }
  }
}

resource "aws_athena_workgroup" "project" {
  name          = var.project_name
  force_destroy = true
  configuration {
    enforce_workgroup_configuration    = true
    publish_cloudwatch_metrics_enabled = true
    bytes_scanned_cutoff_per_query     = 1073741824
    engine_version { selected_engine_version = "Athena engine version 3" }
    result_configuration {
      output_location = "s3://${aws_s3_bucket.project.id}/athena-results/"
      encryption_configuration { encryption_option = "SSE_S3" }
    }
  }
}

resource "aws_athena_named_query" "views" {
  for_each  = fileset("${path.module}/../sql", "*.sql")
  name      = "${var.project_name}-${trimsuffix(each.value, ".sql")}"
  database  = aws_glue_catalog_database.analytics.name
  workgroup = aws_athena_workgroup.project.name
  query     = templatefile("${path.module}/../sql/${each.value}", { database = local.database, archive_day = var.archive_day })
}
