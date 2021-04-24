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
  bucket_prefix = var.prefix
  acl           = "private"

  versioning {
    enabled = true
  }
}

// Uploads the main.py script to the S3 bucket
resource "aws_s3_bucket_object" "python_script" {
  bucket       = aws_s3_bucket.script_bucket.id
  key          = "python_script"
  source       = "${path.module}/main.py"
  acl          = "private"
  content_type = "text/x-script.python"

  depends_on = [
    aws_s3_bucket.script_bucket,
  ]
}

// Zips the mappings folder into mappings.zip
resource "null_resource" "zip" {
  triggers = {
    bucket_prefix = var.prefix
  }

  provisioner "local-exec" {
    command = "zip -r mappings.zip mappings"
  }
}

// Uploads the library to the S3 bucket
resource "aws_s3_bucket_object" "zip_library" {
  bucket       = aws_s3_bucket.script_bucket.id
  key          = "library"
  source       = "${path.module}/mappings.zip"
  acl          = "private"
  content_type = "application/zip"

  depends_on = [
    aws_s3_bucket.script_bucket,
  ]
}