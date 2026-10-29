data "aws_iam_policy_document" "redshift_assume" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["redshift.amazonaws.com", "redshift-serverless.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "redshift" {
  name               = "${var.project}-redshift"
  assume_role_policy = data.aws_iam_policy_document.redshift_assume.json
}

# COPY reads curated Parquet; Spectrum also reads table definitions from the Glue Data Catalog.
data "aws_iam_policy_document" "redshift" {
  statement {
    actions   = ["s3:ListBucket", "s3:GetBucketLocation"]
    resources = [aws_s3_bucket.lake.arn]
  }
  statement {
    actions   = ["s3:GetObject"]
    resources = ["${aws_s3_bucket.lake.arn}/${local.curated_prefix}/*"]
  }
  statement {
    actions = [
      "glue:GetDatabase", "glue:GetDatabases", "glue:GetTable", "glue:GetTables",
      "glue:GetPartition", "glue:GetPartitions", "glue:BatchGetPartition",
    ]
    resources = [
      "arn:${local.partition}:glue:${var.region}:${local.account_id}:catalog",
      aws_glue_catalog_database.curated.arn,
      "arn:${local.partition}:glue:${var.region}:${local.account_id}:table/${aws_glue_catalog_database.curated.name}/*",
    ]
  }
}

resource "aws_iam_role_policy" "redshift" {
  name   = "curated-read"
  role   = aws_iam_role.redshift.id
  policy = data.aws_iam_policy_document.redshift.json
}

resource "aws_redshiftserverless_namespace" "warehouse" {
  namespace_name        = "${var.project}-warehouse"
  db_name               = local.warehouse_db
  admin_username        = "admin"
  manage_admin_password = true
  iam_roles             = [aws_iam_role.redshift.arn]
  default_iam_role_arn  = aws_iam_role.redshift.arn
}

resource "aws_redshiftserverless_workgroup" "warehouse" {
  workgroup_name      = "${var.project}-warehouse"
  namespace_name      = aws_redshiftserverless_namespace.warehouse.namespace_name
  base_capacity       = var.redshift_base_capacity
  publicly_accessible = false
}

resource "aws_redshiftserverless_usage_limit" "daily_compute" {
  resource_arn  = aws_redshiftserverless_workgroup.warehouse.arn
  usage_type    = "serverless-compute"
  amount        = var.redshift_daily_rpu_hours
  period        = "daily"
  breach_action = "deactivate"
}
