resource "aws_athena_workgroup" "analytics" {
  name          = "${var.project}-analytics"
  description   = "Ad hoc SQL on the F1 data lake"
  force_destroy = true

  configuration {
    enforce_workgroup_configuration    = true
    publish_cloudwatch_metrics_enabled = true
    bytes_scanned_cutoff_per_query     = 1073741824 # 1 GB: a runaway query can't cost more than about half a cent

    result_configuration {
      output_location = "s3://${aws_s3_bucket.lake.bucket}/${local.athena_prefix}/"
      encryption_configuration {
        encryption_option = "SSE_S3"
      }
    }
  }
}

resource "aws_athena_named_query" "season_points" {
  name        = "season-points-by-driver"
  workgroup   = aws_athena_workgroup.analytics.id
  database    = aws_glue_catalog_database.curated.name
  description = "Total points per driver per season, straight from the lake"
  query       = <<-SQL
    SELECT season, driver_id, SUM(points) AS points
    FROM results
    GROUP BY season, driver_id
    ORDER BY season DESC, points DESC
  SQL
}

resource "aws_athena_named_query" "rejected_rows" {
  name        = "rejected-rows-by-reason"
  workgroup   = aws_athena_workgroup.analytics.id
  database    = aws_glue_catalog_database.curated.name
  description = "What the transform rejected, and why"
  query       = <<-SQL
    SELECT reject_reason, COUNT(*) AS row_count
    FROM rejected_results
    GROUP BY reject_reason
    ORDER BY row_count DESC
  SQL
}
