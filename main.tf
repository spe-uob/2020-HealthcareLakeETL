terraform {
  required_providers {
    aws = {
      source = "hashicorp/aws"
    }
  }
}
provider "aws" {
  region = var.region
}

resource "aws_s3_bucket" "script_bucket" {
  bucket = "${var.prefix}-etl-scripts"
  acl    = "private"

  versioning {
    enabled = true
  }
}

// Uploads the main.py script to the S3 bucket
resource "aws_s3_bucket_object" "python_script" {
  bucket       = aws_s3_bucket.script_bucket.id
  key          = "main.py"
  source       = "${path.module}/main.py"
  acl          = "private"
  content_type = "text/x-script.python"
}

// Zips the mappings folder into mappings.zip
data "archive_file" "lib" {
  type        = "zip"
  source_dir  = "${path.module}/mappings"
  output_path = "${path.module}/mappings.zip"
}

// Uploads the library to the S3 bucket
resource "aws_s3_bucket_object" "zip_library" {
  bucket       = aws_s3_bucket.script_bucket.id
  key          = "mappings.zip"
  source       = data.archive_file.lib.output_path
  acl          = "private"
  content_type = "application/zip"
}