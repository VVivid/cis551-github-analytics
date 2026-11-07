variable "aws_region" {
  description = "AWS region for every project resource, including QuickSight."
  type        = string
  default     = "us-east-2"
}

variable "project_name" {
  description = "Unique project prefix; changing it creates a separate stack."
  type        = string
  default     = "cis551-github-analytics"
  validation {
    condition     = can(regex("^[a-z][a-z0-9-]{2,25}$", var.project_name))
    error_message = "Use 3-26 lowercase letters, digits and hyphens, starting with a letter."
  }
}

variable "archive_day" {
  description = "One full UTC calendar day in GH Archive."
  type        = string
  default     = "2024-10-15"
  validation {
    condition     = can(formatdate("YYYY-MM-DD", "${var.archive_day}T00:00:00Z"))
    error_message = "archive_day must be a real YYYY-MM-DD date."
  }
}

variable "quicksight_user_arn" {
  description = "Existing QuickSight AUTHOR or ADMIN user ARN in this account and region. Account subscription and user enrollment are separate prerequisites."
  type        = string
  default     = null
  validation {
    condition     = var.quicksight_user_arn == null || can(regex("^arn:[^:]+:quicksight:[^:]+:[0-9]{12}:user/[^/]+/.+$", var.quicksight_user_arn))
    error_message = "Supply an existing QuickSight user ARN. This stack never enrolls or cancels a subscription."
  }
}

variable "enable_quicksight" {
  description = "Create the working dashboard after QuickSight account access and an existing AUTHOR/ADMIN user are available. Core ingestion/ETL/Athena can be deployed independently."
  type        = bool
  default     = false
}

variable "allow_bucket_destroy" {
  description = "Set true only during deliberate teardown to delete this project's bucket contents."
  type        = bool
  default     = false
}
