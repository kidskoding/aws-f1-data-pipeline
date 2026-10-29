output "data_bucket" {
  value = aws_s3_bucket.lake.bucket
}

output "state_machine_arn" {
  value = aws_sfn_state_machine.pipeline.arn
}

output "workgroup_name" {
  value = aws_redshiftserverless_workgroup.warehouse.workgroup_name
}

output "database_name" {
  value = aws_redshiftserverless_namespace.warehouse.db_name
}

output "admin_secret_arn" {
  value = aws_redshiftserverless_namespace.warehouse.admin_password_secret_arn
}

output "athena_workgroup" {
  value = aws_athena_workgroup.analytics.name
}

output "glue_job" {
  value = aws_glue_job.transform.name
}

output "curated_database" {
  value = aws_glue_catalog_database.curated.name
}
