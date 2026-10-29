# Glue: a Data Catalog database per zone, a crawler that discovers the raw JSON's
# schema, and the Spark job that builds the curated tables.

resource "aws_glue_catalog_database" "raw" {
  name        = local.raw_database
  description = "Raw F1 API responses, discovered by the crawler"
}

resource "aws_glue_catalog_database" "curated" {
  name        = local.curated_database
  description = "Clean, typed F1 tables written by the transform job"
}

data "aws_iam_policy_document" "glue_assume" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["glue.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "glue" {
  name               = "${var.project}-glue"
  assume_role_policy = data.aws_iam_policy_document.glue_assume.json
}

resource "aws_iam_role_policy_attachment" "glue_service" {
  role       = aws_iam_role.glue.name
  policy_arn = "arn:${local.partition}:iam::aws:policy/service-role/AWSGlueServiceRole"
}

# Least privilege on the lake: read raw and scripts, write curated, rejects and temp files.
data "aws_iam_policy_document" "glue_lake" {
  statement {
    actions   = ["s3:ListBucket"]
    resources = [aws_s3_bucket.lake.arn]
  }
  statement {
    actions = ["s3:GetObject"]
    resources = [
      "${aws_s3_bucket.lake.arn}/${local.raw_prefix}/*",
      "${aws_s3_bucket.lake.arn}/${local.scripts_prefix}/*",
    ]
  }
  statement {
    actions = ["s3:GetObject", "s3:PutObject", "s3:DeleteObject"]
    resources = [
      "${aws_s3_bucket.lake.arn}/${local.curated_prefix}/*",
      "${aws_s3_bucket.lake.arn}/${local.rejects_prefix}/*",
      "${aws_s3_bucket.lake.arn}/tmp/*",
    ]
  }
}

resource "aws_iam_role_policy" "glue_lake" {
  name   = "lake-access"
  role   = aws_iam_role.glue.id
  policy = data.aws_iam_policy_document.glue_lake.json
}

resource "aws_glue_crawler" "raw" {
  name          = "${var.project}-raw"
  role          = aws_iam_role.glue.arn
  database_name = aws_glue_catalog_database.raw.name
  table_prefix  = "api_"
  description   = "Discovers the schema of raw API responses so they can be browsed in Athena"

  dynamic "s3_target" {
    for_each = ["results", "sprint", "driver_standings"]
    content {
      path = "s3://${aws_s3_bucket.lake.bucket}/${local.raw_prefix}/${s3_target.value}/"
    }
  }

  schema_change_policy {
    update_behavior = "UPDATE_IN_DATABASE"
    delete_behavior = "LOG"
  }

  recrawl_policy {
    recrawl_behavior = "CRAWL_EVERYTHING"
  }
}

resource "aws_s3_object" "transform_script" {
  bucket = aws_s3_bucket.lake.id
  key    = "${local.scripts_prefix}/transform_f1.py"
  source = "${path.module}/../glue/transform_f1.py"
  etag   = filemd5("${path.module}/../glue/transform_f1.py")
}

resource "aws_glue_job" "transform" {
  name              = "${var.project}-transform"
  description       = "Raw F1 JSON to curated Parquet, with rejects, reconciliation and Data Quality rules"
  role_arn          = aws_iam_role.glue.arn
  glue_version      = "5.0"
  worker_type       = "G.1X"
  number_of_workers = 2  # the smallest Spark job Glue allows
  timeout           = 15 # minutes; a cost guard
  max_retries       = 0

  command {
    name            = "glueetl"
    python_version  = "3"
    script_location = "s3://${aws_s3_bucket.lake.bucket}/${aws_s3_object.transform_script.key}"
  }

  execution_property {
    max_concurrent_runs = 1
  }

  default_arguments = {
    "--raw_path"                         = "s3://${aws_s3_bucket.lake.bucket}/${local.raw_prefix}"
    "--curated_path"                     = "s3://${aws_s3_bucket.lake.bucket}/${local.curated_prefix}"
    "--rejects_path"                     = "s3://${aws_s3_bucket.lake.bucket}/${local.rejects_prefix}/results"
    "--catalog_db"                       = aws_glue_catalog_database.curated.name
    "--TempDir"                          = "s3://${aws_s3_bucket.lake.bucket}/tmp/glue/"
    "--job-language"                     = "python"
    "--enable-metrics"                   = "true"
    "--enable-observability-metrics"     = "true"
    "--enable-continuous-cloudwatch-log" = "true"
  }
}
