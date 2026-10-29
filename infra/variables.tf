variable "project" {
  description = "Name prefix for every resource."
  type        = string
  default     = "f1-pipeline"
}

variable "region" {
  description = "AWS region to deploy into."
  type        = string
  default     = "us-east-1"
}

variable "notification_email" {
  description = "Email for pipeline success and failure alerts. Empty means no subscription."
  type        = string
  default     = ""
}

variable "schedule_enabled" {
  description = "Run the pipeline weekly on a schedule. Off by default so nothing runs (or costs money) unattended."
  type        = bool
  default     = false
}

variable "schedule_expression" {
  description = "When the scheduled run fires (UTC). Default: Mondays 12:00, after race weekends."
  type        = string
  default     = "cron(0 12 ? * MON *)"
}

variable "redshift_base_capacity" {
  description = "Redshift Serverless base capacity in RPUs. 4 is the smallest size."
  type        = number
  default     = 4
}

variable "redshift_daily_rpu_hours" {
  description = "Daily compute cap. Past it, the workgroup turns itself off until the next day."
  type        = number
  default     = 4
}

variable "lambda_zip" {
  description = "Path to the Lambda package built by `make package`."
  type        = string
  default     = "../build/lambda.zip"
}

variable "log_retention_days" {
  description = "How long to keep Lambda and Step Functions logs."
  type        = number
  default     = 14
}

variable "force_destroy" {
  description = "Let `terraform destroy` delete a non-empty bucket. Fine for a demo, false in production."
  type        = bool
  default     = true
}
