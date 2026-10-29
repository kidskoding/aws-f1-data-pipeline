data "aws_iam_policy_document" "lambda_assume" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["lambda.amazonaws.com"]
    }
  }
}

locals {
  lambda_functions = {
    extract = {
      handler     = "f1_pipeline.extract.handler.handler"
      description = "Copy F1 data from the Jolpica API into the raw zone"
      timeout     = 600
      environment = {
        DATA_BUCKET = aws_s3_bucket.lake.bucket
        RAW_PREFIX  = local.raw_prefix
      }
    }
    load = {
      handler     = "f1_pipeline.load.handler.handler"
      description = "Load curated Parquet into Redshift Serverless in one transaction"
      timeout     = 600
      environment = {
        WORKGROUP        = aws_redshiftserverless_workgroup.warehouse.workgroup_name
        DATABASE         = aws_redshiftserverless_namespace.warehouse.db_name
        SECRET_ARN       = aws_redshiftserverless_namespace.warehouse.admin_password_secret_arn
        DATA_BUCKET      = aws_s3_bucket.lake.bucket
        CURATED_PREFIX   = local.curated_prefix
        CURATED_DATABASE = aws_glue_catalog_database.curated.name
      }
    }
  }
}

resource "aws_iam_role" "lambda" {
  for_each           = local.lambda_functions
  name               = "${var.project}-${each.key}"
  assume_role_policy = data.aws_iam_policy_document.lambda_assume.json
}

resource "aws_iam_role_policy_attachment" "lambda_basic" {
  for_each   = local.lambda_functions
  role       = aws_iam_role.lambda[each.key].name
  policy_arn = "arn:${local.partition}:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
}

resource "aws_iam_role_policy_attachment" "lambda_xray" {
  for_each   = local.lambda_functions
  role       = aws_iam_role.lambda[each.key].name
  policy_arn = "arn:${local.partition}:iam::aws:policy/AWSXRayDaemonWriteAccess"
}

data "aws_iam_policy_document" "extract" {
  statement {
    actions   = ["s3:ListBucket"]
    resources = [aws_s3_bucket.lake.arn]
    condition {
      test     = "StringLike"
      variable = "s3:prefix"
      values   = ["${local.raw_prefix}/*"]
    }
  }
  statement {
    actions   = ["s3:PutObject", "s3:DeleteObject"]
    resources = ["${aws_s3_bucket.lake.arn}/${local.raw_prefix}/*"]
  }
}

resource "aws_iam_role_policy" "extract" {
  name   = "raw-zone-write"
  role   = aws_iam_role.lambda["extract"].id
  policy = data.aws_iam_policy_document.extract.json
}

data "aws_iam_policy_document" "load" {
  statement {
    actions   = ["redshift-data:ExecuteStatement", "redshift-data:BatchExecuteStatement"]
    resources = [aws_redshiftserverless_workgroup.warehouse.arn]
  }
  statement {
    # Statement IDs aren't known in advance; the Data API scopes these to the caller's own statements.
    actions   = ["redshift-data:DescribeStatement", "redshift-data:GetStatementResult", "redshift-data:CancelStatement"]
    resources = ["*"]
  }
  statement {
    actions   = ["secretsmanager:GetSecretValue"]
    resources = [aws_redshiftserverless_namespace.warehouse.admin_password_secret_arn]
  }
}

resource "aws_iam_role_policy" "load" {
  name   = "warehouse-load"
  role   = aws_iam_role.lambda["load"].id
  policy = data.aws_iam_policy_document.load.json
}

resource "aws_cloudwatch_log_group" "lambda" {
  for_each          = local.lambda_functions
  name              = "/aws/lambda/${var.project}-${each.key}"
  retention_in_days = var.log_retention_days
}

resource "aws_lambda_function" "this" {
  for_each         = local.lambda_functions
  function_name    = "${var.project}-${each.key}"
  description      = each.value.description
  role             = aws_iam_role.lambda[each.key].arn
  handler          = each.value.handler
  runtime          = "python3.13"
  architectures    = ["arm64"] # Graviton: cheaper per millisecond than x86
  memory_size      = 256
  timeout          = each.value.timeout
  filename         = var.lambda_zip
  source_code_hash = filebase64sha256(var.lambda_zip)

  environment {
    variables = merge(each.value.environment, { LOG_LEVEL = "INFO" })
  }

  tracing_config {
    mode = "Active"
  }

  depends_on = [aws_cloudwatch_log_group.lambda]
}
