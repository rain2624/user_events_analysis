terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "6.61.0"
    }
  }
}

provider "aws" {
  region = var.region
}

resource "aws_s3_bucket" "user_interactions_bucket" {
  bucket = var.bucket_name
}

# IAM policy - glue service role
resource "aws_iam_role" "glue_role" {
  name = var.iam-glue-role-name

  assume_role_policy = jsonencode({
    "Version" : "2012-10-17",
    "Statement" : [
      {
        "Effect" : "Allow",
        "Principal" : {
          "Service" : "glue.amazonaws.com"
        },
        "Action" : "sts:AssumeRole"
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "glue_service_role" {
  role       = aws_iam_role.glue_role.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"
}

resource "aws_iam_role_policy_attachment" "glue_secret_manager" {
  role       = aws_iam_role.glue_role.name
  policy_arn = "arn:aws:iam::aws:policy/AWSSecretsManagerClientReadOnlyAccess"
}


# IAM policy - glue access S3 
resource "aws_iam_policy" "glue_s3_policy" {
  name = var.iam-glue-policy-name

  policy = jsonencode({
    "Version" : "2012-10-17",
    "Statement" : [
      {
        "Action" : [
          "s3:GetObject",
          "s3:PutObject"
        ],
        "Effect" : "Allow",
        "Resource" : "${aws_s3_bucket.user_interactions_bucket.arn}/*"
      },

      {
        "Action" : [
          "s3:ListBucket"
        ],
        "Effect" : "Allow",
        Resource : aws_s3_bucket.user_interactions_bucket.arn
      }
    ]

  })
}

resource "aws_iam_role_policy_attachment" "glue_s3_access" {
  role       = aws_iam_role.glue_role.name
  policy_arn = aws_iam_policy.glue_s3_policy.arn
}


# AWS IAM role - Lambda 
resource "aws_iam_role" "lambda_role" {
  name = var.iam-lambda-role-name

  assume_role_policy = jsonencode({
    "Version" : "2012-10-17",
    "Statement" : [
      {
        "Effect" : "Allow",
        "Principal" : {
          "Service" : "lambda.amazonaws.com"
        },
        "Action" : "sts:AssumeRole"
      }
    ]
  })
}

# for cloudwatch
resource "aws_iam_role_policy_attachment" "lambda_basic_execution_role" {
  role       = aws_iam_role.lambda_role.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
}


# IAM policy for S3 and glue workflow
resource "aws_iam_policy" "lambda_glue_policy" {
  name = var.iam-lambda-policy-name

  policy = jsonencode({
    "Version" : "2012-10-17",
    "Statement" : [
      {
        "Action" : [
          "glue:StartWorkflowRun"
        ],
        "Effect" : "Allow",
        Resource : "arn:aws:glue:${var.region}:${var.account_id}:workflow/${var.glue_workflow_name}"
      }
    ]

  })
}

# resource "aws_iam_policy" "lambda_glue_policy" {
#   name = var.iam-lambda-policy-name

#   policy = jsonencode({
#     "Version" : "2012-10-17",
#     "Statement" : [
#       {
#         "Action" : [
#           "glue:StartJobRun"
#         ],
#         "Effect" : "Allow",
#         "Resource" : "arn:aws:glue:${var.region}:${var.account_id}:job/${var.glue_job_name}"
#       }
#     ]
#   })
# }
resource "aws_iam_role_policy_attachment" "lambda_glue_trigger_role" {
  role       = aws_iam_role.lambda_role.name
  policy_arn = aws_iam_policy.lambda_glue_policy.arn
}
