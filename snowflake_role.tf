# terraform/modules/iam/snowflake_role.tf
#
# Two-phase Snowflake IAM trust bootstrap.
#
# WHY TWO PHASES:
# Snowflake generates GLUE_AWS_EXTERNAL_ID and STORAGE_AWS_EXTERNAL_ID
# only AFTER the CATALOG INTEGRATION and EXTERNAL VOLUME objects are created.
# Those objects need the IAM role ARN to be created first.
# → Circular dependency that Terraform can't resolve in one apply.
#
# PHASE 1: Apply with a permissive trust policy (Snowflake account principal only).
#          Create Snowflake objects. Extract external IDs via DESCRIBE commands.
# PHASE 2: Run terraform apply again with external IDs in variables.
#          Trust policy is tightened — scoped to specific external IDs.
#
# Phase 2 closes the security gap: any Snowflake account could assume your role
# without external ID scoping. This is a real exposure, not a theoretical one.

variable "environment"                   { type = string }
variable "data_lake_bucket"              { type = string }
variable "glue_database"                 { type = string }
variable "s3_warehouse_prefix"           { type = string }
variable "snowflake_aws_account_arn"     { type = string }

# Phase 2 variables — empty string in Phase 1, populated after DESCRIBE commands
variable "snowflake_glue_iam_user_arn"   {
  type    = string
  default = ""
  description = "From: DESCRIBE CATALOG INTEGRATION → GLUE_AWS_IAM_USER_ARN"
}
variable "glue_aws_external_id"          {
  type    = string
  default = ""
  description = "From: DESCRIBE CATALOG INTEGRATION → GLUE_AWS_EXTERNAL_ID"
}
variable "snowflake_storage_iam_user_arn" {
  type    = string
  default = ""
  description = "From: DESC EXTERNAL VOLUME → STORAGE_AWS_IAM_USER_ARN"
}
variable "storage_aws_external_id"       {
  type    = string
  default = ""
  description = "From: DESC EXTERNAL VOLUME → STORAGE_AWS_EXTERNAL_ID"
}

locals {
  # Phase detection: if external IDs are populated, use scoped trust (Phase 2)
  # Otherwise use permissive trust (Phase 1)
  phase2_ready = (
    var.glue_aws_external_id != "" &&
    var.storage_aws_external_id != "" &&
    var.snowflake_glue_iam_user_arn != "" &&
    var.snowflake_storage_iam_user_arn != ""
  )
}

# ── Trust policy: Phase 1 (permissive) ───────────────────────────────────
data "aws_iam_policy_document" "snowflake_trust_phase1" {
  statement {
    effect  = "Allow"
    actions = ["sts:AssumeRole"]
    principals {
      type        = "AWS"
      identifiers = [var.snowflake_aws_account_arn]
    }
    # Note: No ExternalId condition here — this is intentionally permissive
    # for bootstrap only. Phase 2 narrows this significantly.
  }
}

# ── Trust policy: Phase 2 (scoped — use after extracting external IDs) ───
data "aws_iam_policy_document" "snowflake_trust_phase2" {
  # Glue Catalog integration trust
  statement {
    effect  = "Allow"
    actions = ["sts:AssumeRole"]
    principals {
      type        = "AWS"
      identifiers = [var.snowflake_glue_iam_user_arn]
    }
    condition {
      test     = "StringEquals"
      variable = "sts:ExternalId"
      values   = [var.glue_aws_external_id]
    }
  }

  # S3 External Volume trust
  statement {
    effect  = "Allow"
    actions = ["sts:AssumeRole"]
    principals {
      type        = "AWS"
      identifiers = [var.snowflake_storage_iam_user_arn]
    }
    condition {
      test     = "StringEquals"
      variable = "sts:ExternalId"
      values   = [var.storage_aws_external_id]
    }
  }
}

# ── IAM Role ──────────────────────────────────────────────────────────────
resource "aws_iam_role" "snowflake_service_role" {
  name = "snowflake-service-role-${var.environment}"

  assume_role_policy = local.phase2_ready ? (
    data.aws_iam_policy_document.snowflake_trust_phase2.json
  ) : (
    data.aws_iam_policy_document.snowflake_trust_phase1.json
  )

  tags = {
    Environment = var.environment
    ManagedBy   = "terraform"
    Phase       = local.phase2_ready ? "2-scoped" : "1-bootstrap"
  }
}

# ── Permissions: Glue Catalog read access ────────────────────────────────
data "aws_iam_policy_document" "snowflake_glue_policy" {
  statement {
    sid    = "GlueCatalogRead"
    effect = "Allow"
    actions = [
      "glue:GetDatabase",
      "glue:GetTable",
      "glue:GetTables",
      "glue:GetPartition",
      "glue:GetPartitions",
    ]
    resources = [
      "arn:aws:glue:*:*:catalog",
      "arn:aws:glue:*:*:database/${var.glue_database}",
      "arn:aws:glue:*:*:table/${var.glue_database}/*",
    ]
  }

  statement {
    sid    = "S3IcebergRead"
    effect = "Allow"
    actions = [
      "s3:GetObject",
      "s3:ListBucket",
      "s3:GetBucketLocation",
    ]
    resources = [
      "arn:aws:s3:::${var.data_lake_bucket}",
      "arn:aws:s3:::${var.data_lake_bucket}/${var.s3_warehouse_prefix}/*",
    ]
  }
}

resource "aws_iam_policy" "snowflake_policy" {
  name   = "snowflake-service-policy-${var.environment}"
  policy = data.aws_iam_policy_document.snowflake_glue_policy.json
}

resource "aws_iam_role_policy_attachment" "snowflake_policy" {
  role       = aws_iam_role.snowflake_service_role.name
  policy_arn = aws_iam_policy.snowflake_policy.arn
}

output "snowflake_service_role_arn" {
  value       = aws_iam_role.snowflake_service_role.arn
  description = "Use this ARN in Snowflake CATALOG INTEGRATION and EXTERNAL VOLUME"
}

output "is_phase2" {
  value       = local.phase2_ready
  description = "true = Phase 2 scoped trust applied. false = Phase 1 bootstrap."
}
