locals {
  dashboard_datasets = {
    hourly = {
      query   = "SELECT hour_utc, opening_count FROM \"${local.database}\".hourly_status WHERE processing_status = 'COMPLETED'"
      columns = { hour_utc = "STRING", opening_count = "INTEGER" }
    }
    repositories = {
      query   = "SELECT repository, opening_count FROM \"${local.database}\".repository_totals WHERE repository_rank <= 10"
      columns = { repository = "STRING", opening_count = "INTEGER" }
    }
    concentration = {
      query   = "SELECT repository, share_percent FROM \"${local.database}\".busiest_hour_repositories WHERE repository_rank <= 10"
      columns = { repository = "STRING", share_percent = "DECIMAL" }
    }
    overview = {
      query   = "SELECT report_status, completed_hours, missing_hours, total_openings, busiest_hour_utc, top_10_share_percent, busiest_top_repository, busiest_top_repository_share_percent FROM \"${local.database}\".overview"
      columns = { report_status = "STRING", completed_hours = "INTEGER", missing_hours = "INTEGER", total_openings = "INTEGER", busiest_hour_utc = "STRING", top_10_share_percent = "DECIMAL", busiest_top_repository = "STRING", busiest_top_repository_share_percent = "DECIMAL" }
    }
    coverage = {
      query   = "SELECT hour_utc, processing_status, opening_count, raw_events, rejected_openings, duplicate_openings, completed_at FROM \"${local.database}\".hourly_status"
      columns = { hour_utc = "STRING", processing_status = "STRING", opening_count = "INTEGER", raw_events = "INTEGER", rejected_openings = "INTEGER", duplicate_openings = "INTEGER", completed_at = "STRING" }
    }
  }
  bar_charts = {
    hourly        = { title = "PR openings by archive hour (UTC)", category = "hour_utc", value = "opening_count", orientation = "VERTICAL", sort = "ASC", sort_field = "hourly-category", column = 0, row = 8, width = 36, height = 14 }
    repositories  = { title = "Top 10 repositories across completed hours", category = "repository", value = "opening_count", orientation = "HORIZONTAL", sort = "DESC", sort_field = "repositories-value", column = 0, row = 22, width = 18, height = 18 }
    concentration = { title = "Busiest hour: top repositories' share (%)", category = "repository", value = "share_percent", orientation = "HORIZONTAL", sort = "DESC", sort_field = "concentration-value", column = 18, row = 22, width = 18, height = 18 }
  }
  summary_columns  = ["report_status", "completed_hours", "missing_hours", "total_openings", "busiest_hour_utc", "top_10_share_percent", "busiest_top_repository", "busiest_top_repository_share_percent"]
  coverage_columns = ["hour_utc", "processing_status", "opening_count", "raw_events", "rejected_openings", "duplicate_openings", "completed_at"]
}

resource "aws_iam_role" "quicksight" {
  count = var.enable_quicksight ? 1 : 0
  name  = "${var.project_name}-quicksight"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow", Principal = { Service = "quicksight.amazonaws.com" }, Action = "sts:AssumeRole"
      Condition = { StringEquals = { "aws:SourceAccount" = local.account_id } }
    }]
  })
}

resource "aws_iam_role_policy" "quicksight" {
  count = var.enable_quicksight ? 1 : 0
  role  = aws_iam_role.quicksight[0].id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      { Effect = "Allow", Action = ["athena:StartQueryExecution", "athena:StopQueryExecution", "athena:GetQueryExecution", "athena:GetQueryResults", "athena:GetWorkGroup"], Resource = aws_athena_workgroup.project.arn },
      { Effect = "Allow", Action = ["athena:GetDataCatalog"], Resource = "arn:${local.partition}:athena:${var.aws_region}:${local.account_id}:datacatalog/AwsDataCatalog" },
      { Effect = "Allow", Action = ["glue:GetDatabase", "glue:GetDatabases", "glue:GetTable", "glue:GetTables", "glue:GetPartition", "glue:GetPartitions", "glue:BatchGetPartition"], Resource = ["arn:${local.partition}:glue:${var.aws_region}:${local.account_id}:catalog", aws_glue_catalog_database.analytics.arn, "arn:${local.partition}:glue:${var.aws_region}:${local.account_id}:table/${local.database}/*"] },
      { Effect = "Allow", Action = ["s3:GetBucketLocation"], Resource = aws_s3_bucket.project.arn },
      { Effect = "Allow", Action = ["s3:ListBucket"], Resource = aws_s3_bucket.project.arn, Condition = { StringLike = { "s3:prefix" = ["curated/*", "athena-results/*"] } } },
      { Effect = "Allow", Action = ["s3:GetObject"], Resource = ["${aws_s3_bucket.project.arn}/curated/*", "${aws_s3_bucket.project.arn}/athena-results/*"] },
      { Effect = "Allow", Action = ["s3:PutObject", "s3:AbortMultipartUpload", "s3:ListMultipartUploadParts"], Resource = "${aws_s3_bucket.project.arn}/athena-results/*" }
    ]
  })
}

resource "aws_quicksight_data_source" "athena" {
  count          = var.enable_quicksight ? 1 : 0
  data_source_id = var.project_name
  name           = "CIS551 GitHub analytics"
  type           = "ATHENA"
  parameters {
    athena {
      work_group = aws_athena_workgroup.project.name
      role_arn   = aws_iam_role.quicksight[0].arn
    }
  }
  permission {
    principal = var.quicksight_user_arn
    actions   = ["quicksight:DescribeDataSource", "quicksight:DescribeDataSourcePermissions", "quicksight:PassDataSource", "quicksight:UpdateDataSource", "quicksight:DeleteDataSource", "quicksight:UpdateDataSourcePermissions"]
  }
  lifecycle {
    precondition {
      condition     = var.quicksight_user_arn != null
      error_message = "Set quicksight_user_arn to an existing AUTHOR/ADMIN user before enabling QuickSight."
    }
  }
  depends_on = [aws_iam_role_policy.quicksight]
}

resource "aws_quicksight_data_set" "analytics" {
  for_each    = var.enable_quicksight ? local.dashboard_datasets : {}
  data_set_id = "${var.project_name}-${each.key}"
  name        = "CIS551 ${each.key}"
  import_mode = "DIRECT_QUERY"
  physical_table_map {
    physical_table_map_id = each.key
    custom_sql {
      data_source_arn = aws_quicksight_data_source.athena[0].arn
      name            = each.key
      sql_query       = each.value.query
      dynamic "columns" {
        for_each = each.value.columns
        content {
          name = columns.key
          type = columns.value
        }
      }
    }
  }
  permissions {
    principal = var.quicksight_user_arn
    actions   = ["quicksight:DescribeDataSet", "quicksight:DescribeDataSetPermissions", "quicksight:PassDataSet", "quicksight:DescribeIngestion", "quicksight:ListIngestions", "quicksight:UpdateDataSet", "quicksight:DeleteDataSet", "quicksight:CreateIngestion", "quicksight:CancelIngestion", "quicksight:UpdateDataSetPermissions"]
  }
}

resource "aws_quicksight_dashboard" "analytics" {
  count               = var.enable_quicksight ? 1 : 0
  dashboard_id        = var.project_name
  name                = "GitHub PR openings · ${var.archive_day} UTC"
  version_description = "CIS551 reproducible GH Archive analysis"
  permissions {
    principal = var.quicksight_user_arn
    actions   = ["quicksight:DescribeDashboard", "quicksight:ListDashboardVersions", "quicksight:UpdateDashboardPermissions", "quicksight:QueryDashboard", "quicksight:UpdateDashboard", "quicksight:DeleteDashboard", "quicksight:DescribeDashboardPermissions", "quicksight:UpdateDashboardPublishedVersion"]
  }
  definition {
    dynamic "data_set_identifiers_declarations" {
      for_each = aws_quicksight_data_set.analytics
      content {
        identifier   = data_set_identifiers_declarations.key
        data_set_arn = data_set_identifiers_declarations.value.arn
      }
    }
    sheets {
      sheet_id = "analysis"
      name     = "PR opening patterns"
      title    = "${var.archive_day} UTC · Full-day conclusions require 24 completed hours"
      visuals {
        table_visual {
          visual_id = "overview"
          title {
            format_text { plain_text = "Coverage and concentration · percentages use completed hours" }
          }
          chart_configuration {
            field_wells {
              table_unaggregated_field_wells {
                dynamic "values" {
                  for_each = local.summary_columns
                  content {
                    field_id = "summary-${values.value}"
                    column {
                      data_set_identifier = "overview"
                      column_name         = values.value
                    }
                  }
                }
              }
            }
          }
        }
      }
      dynamic "visuals" {
        for_each = local.bar_charts
        content {
          bar_chart_visual {
            visual_id = visuals.key
            title {
              format_text { plain_text = visuals.value.title }
            }
            chart_configuration {
              orientation = visuals.value.orientation
              field_wells {
                bar_chart_aggregated_field_wells {
                  category {
                    categorical_dimension_field {
                      field_id = "${visuals.key}-category"
                      column {
                        data_set_identifier = visuals.key
                        column_name         = visuals.value.category
                      }
                    }
                  }
                  values {
                    numerical_measure_field {
                      field_id = "${visuals.key}-value"
                      column {
                        data_set_identifier = visuals.key
                        column_name         = visuals.value.value
                      }
                      aggregation_function { simple_numerical_aggregation = "SUM" }
                    }
                  }
                }
              }
              sort_configuration {
                category_sort {
                  field_sort {
                    field_id  = visuals.value.sort_field
                    direction = visuals.value.sort
                  }
                }
              }
            }
          }
        }
      }
      layouts {
        configuration {
          grid_layout {
            canvas_size_options {
              screen_canvas_size_options {
                resize_option             = "FIXED"
                optimized_view_port_width = "1440px"
              }
            }
            elements {
              element_id   = "overview"
              element_type = "VISUAL"
              column_index = 0
              row_index    = 0
              column_span  = 36
              row_span     = 8
            }
            dynamic "elements" {
              for_each = local.bar_charts
              content {
                element_id   = elements.key
                element_type = "VISUAL"
                column_index = elements.value.column
                row_index    = elements.value.row
                column_span  = elements.value.width
                row_span     = elements.value.height
              }
            }
          }
        }
      }
    }
    sheets {
      sheet_id = "coverage"
      name     = "Hourly completion"
      title    = "Missing means unprocessed or failed; completed zero means no valid openings"
      visuals {
        table_visual {
          visual_id = "coverage"
          title {
            format_text { plain_text = "All 24 archive hours (UTC)" }
          }
          chart_configuration {
            field_wells {
              table_unaggregated_field_wells {
                dynamic "values" {
                  for_each = local.coverage_columns
                  content {
                    field_id = "coverage-${values.value}"
                    column {
                      data_set_identifier = "coverage"
                      column_name         = values.value
                    }
                  }
                }
              }
            }
            sort_configuration {
              row_sort {
                field_sort {
                  field_id  = "coverage-hour_utc"
                  direction = "ASC"
                }
              }
              pagination_configuration {
                page_number = 1
                page_size   = 100
              }
            }
          }
        }
      }
      layouts {
        configuration {
          grid_layout {
            elements {
              element_id   = "coverage"
              element_type = "VISUAL"
              column_index = 0
              row_index    = 0
              column_span  = 36
              row_span     = 32
            }
          }
        }
      }
    }
  }
}

output "dashboard_url" {
  value = var.enable_quicksight ? "https://${var.aws_region}.quicksight.aws.amazon.com/sn/dashboards/${aws_quicksight_dashboard.analytics[0].dashboard_id}" : null
}
