resource "aws_sns_topic" "pipeline" {
  name              = "${var.project}-notifications"
  kms_master_key_id = "alias/aws/sns"
}

resource "aws_sns_topic_subscription" "email" {
  count     = var.notification_email == "" ? 0 : 1
  topic_arn = aws_sns_topic.pipeline.arn
  protocol  = "email"
  endpoint  = var.notification_email # AWS emails a confirmation link first
}

data "aws_iam_policy_document" "states_assume" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["states.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "states" {
  name               = "${var.project}-states"
  assume_role_policy = data.aws_iam_policy_document.states_assume.json
}

data "aws_iam_policy_document" "states" {
  statement {
    actions   = ["lambda:InvokeFunction"]
    resources = [for f in aws_lambda_function.this : f.arn]
  }
  statement {
    actions   = ["glue:StartJobRun", "glue:GetJobRun", "glue:GetJobRuns", "glue:BatchStopJobRun"]
    resources = [aws_glue_job.transform.arn]
  }
  statement {
    actions   = ["glue:StartCrawler"]
    resources = [aws_glue_crawler.raw.arn]
  }
  statement {
    actions = [
      "athena:StartQueryExecution", "athena:GetQueryExecution", "athena:GetQueryResults",
      "athena:StopQueryExecution", "athena:GetWorkGroup",
    ]
    resources = [aws_athena_workgroup.analytics.arn]
  }
  statement {
    # Athena reads the catalog and the data, and writes results, as the caller.
    actions = ["glue:GetDatabase", "glue:GetTable", "glue:GetPartitions"]
    resources = [
      "arn:${local.partition}:glue:${var.region}:${local.account_id}:catalog",
      aws_glue_catalog_database.curated.arn,
      "arn:${local.partition}:glue:${var.region}:${local.account_id}:table/${aws_glue_catalog_database.curated.name}/*",
    ]
  }
  statement {
    actions   = ["s3:GetBucketLocation", "s3:ListBucket"]
    resources = [aws_s3_bucket.lake.arn]
  }
  statement {
    actions   = ["s3:GetObject"]
    resources = ["${aws_s3_bucket.lake.arn}/${local.curated_prefix}/*"]
  }
  statement {
    actions   = ["s3:GetObject", "s3:PutObject", "s3:AbortMultipartUpload"]
    resources = ["${aws_s3_bucket.lake.arn}/${local.athena_prefix}/*"]
  }
  statement {
    actions   = ["sns:Publish"]
    resources = [aws_sns_topic.pipeline.arn]
  }
  statement {
    # Required for .sync Glue integrations and for log delivery; these APIs don't support resource scoping.
    actions = [
      "events:PutTargets", "events:PutRule", "events:DescribeRule",
      "logs:CreateLogDelivery", "logs:GetLogDelivery", "logs:UpdateLogDelivery", "logs:DeleteLogDelivery",
      "logs:ListLogDeliveries", "logs:PutResourcePolicy", "logs:DescribeResourcePolicies", "logs:DescribeLogGroups",
      "xray:PutTraceSegments", "xray:PutTelemetryRecords", "xray:GetSamplingRules", "xray:GetSamplingTargets",
    ]
    resources = ["*"]
  }
}

resource "aws_iam_role_policy" "states" {
  name   = "pipeline"
  role   = aws_iam_role.states.id
  policy = data.aws_iam_policy_document.states.json
}

resource "aws_cloudwatch_log_group" "states" {
  name              = "/aws/vendedlogs/states/${var.project}-pipeline"
  retention_in_days = var.log_retention_days
}

resource "aws_sfn_state_machine" "pipeline" {
  name     = "${var.project}-pipeline"
  role_arn = aws_iam_role.states.arn
  definition = templatefile("${path.module}/statemachine/pipeline.asl.json", {
    extract_function_arn = aws_lambda_function.this["extract"].arn
    load_function_arn    = aws_lambda_function.this["load"].arn
    transform_job_name   = aws_glue_job.transform.name
    raw_crawler_name     = aws_glue_crawler.raw.name
    curated_database     = aws_glue_catalog_database.curated.name
    athena_workgroup     = aws_athena_workgroup.analytics.name
    topic_arn            = aws_sns_topic.pipeline.arn
  })

  logging_configuration {
    log_destination        = "${aws_cloudwatch_log_group.states.arn}:*"
    include_execution_data = false
    level                  = "ERROR"
  }

  tracing_configuration {
    enabled = true
  }

  depends_on = [aws_iam_role_policy.states]
}
