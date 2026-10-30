# Cloud twin of the local stack. NOT needed to run locally; use it to show IaC skills / deploy to AWS.

terraform {
  required_version = ">= 1.5"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
    random = {
      source  = "hashicorp/random"
      version = "~> 3.5"
    }
  }
}

provider "aws" {
  region = var.region
}

locals {
  layers = ["bronze", "silver", "gold", "quarantine", "athena-results"]
}

resource "random_id" "suffix" {
  byte_length = 3
}

# ---- Storage: one bucket per medallion layer, encrypted, versioned, private ----

resource "aws_s3_bucket" "layer" {
  for_each = toset(local.layers)

  bucket = "${var.project}-${each.key}-${random_id.suffix.hex}"

  tags = {
    Project = var.project
    Layer   = each.key
  }
}

resource "aws_s3_bucket_versioning" "layer" {
  for_each = aws_s3_bucket.layer

  bucket = each.value.id

  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "layer" {
  for_each = aws_s3_bucket.layer

  bucket = each.value.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_s3_bucket_public_access_block" "layer" {
  for_each = aws_s3_bucket.layer

  bucket                  = each.value.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# ---- Network ----

resource "aws_vpc" "main" {
  cidr_block           = "10.20.0.0/16"
  enable_dns_hostnames = true

  tags = {
    Name = "${var.project}-vpc"
  }
}

data "aws_availability_zones" "az" {}

resource "aws_subnet" "private" {
  count = 2

  vpc_id            = aws_vpc.main.id
  cidr_block        = cidrsubnet(aws_vpc.main.cidr_block, 8, count.index)
  availability_zone = data.aws_availability_zones.az.names[count.index]

  tags = {
    Name = "${var.project}-private-${count.index}"
  }
}

resource "aws_security_group" "pipeline" {
  name_prefix = "${var.project}-pipeline-"
  vpc_id      = aws_vpc.main.id

  ingress {
    description = "intra-SG traffic (Kafka/Spark)"
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    self        = true
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}

# ---- IAM: role for Spark/Airflow workloads ----

data "aws_iam_policy_document" "assume" {
  statement {
    actions = ["sts:AssumeRole"]

    principals {
      type        = "Service"
      identifiers = ["ecs-tasks.amazonaws.com", "glue.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "pipeline" {
  name               = "${var.project}-pipeline-role"
  assume_role_policy = data.aws_iam_policy_document.assume.json
}

data "aws_iam_policy_document" "lake_access" {
  statement {
    actions = [
      "s3:GetObject",
      "s3:PutObject",
      "s3:DeleteObject",
      "s3:ListBucket",
      "s3:GetBucketLocation"
    ]

    resources = flatten([
      for b in aws_s3_bucket.layer : [
        b.arn,
        "${b.arn}/*"
      ]
    ])
  }

  statement {
    actions = [
      "glue:Get*",
      "glue:CreateTable",
      "glue:UpdateTable",
      "athena:StartQueryExecution",
      "athena:GetQueryExecution",
      "athena:GetQueryResults"
    ]

    resources = ["*"]
  }
}

resource "aws_iam_role_policy" "lake_access" {
  role   = aws_iam_role.pipeline.id
  policy = data.aws_iam_policy_document.lake_access.json
}

# ---- Serving: Glue catalog + Athena workgroup ----

resource "aws_glue_catalog_database" "lake" {
  name = replace("${var.project}_lake", "-", "_")
}

resource "aws_athena_workgroup" "main" {
  name = "${var.project}-wg"

  configuration {
    enforce_workgroup_configuration = true

    result_configuration {
      output_location = "s3://${aws_s3_bucket.layer["athena-results"].bucket}/results/"
    }
  }
}

# ---- Optional managed Kafka (costs money, off by default) ----

resource "aws_msk_serverless_cluster" "kafka" {
  count = var.enable_msk ? 1 : 0

  cluster_name = "${var.project}-kafka"

  vpc_config {
    subnet_ids         = aws_subnet.private[*].id
    security_group_ids = [aws_security_group.pipeline.id]
  }

  client_authentication {
    sasl {
      iam {
        enabled = true
      }
    }
  }
}