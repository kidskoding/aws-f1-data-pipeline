data "aws_caller_identity" "current" {}
data "aws_partition" "current" {}

locals {
  account_id = data.aws_caller_identity.current.account_id
  partition  = data.aws_partition.current.partition
  bucket     = "${var.project}-lake-${local.account_id}"

  raw_prefix     = "raw"
  curated_prefix = "curated"
  rejects_prefix = "rejects"
  athena_prefix  = "athena-results"
  scripts_prefix = "scripts"

  raw_database     = replace("${var.project}_raw", "-", "_")
  curated_database = replace("${var.project}_curated", "-", "_")
  warehouse_db     = "f1"
}
