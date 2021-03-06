output "s3_bucket_name" {
  value = aws_s3_bucket.script_bucket.id
}

output "script_path" {
  value = "s3://${aws_s3_bucket.script_bucket.id}/${aws_s3_bucket_object.python_script.id}"
}

output "library_path" {
  value = "s3://${aws_s3_bucket.script_bucket.id}/${aws_s3_bucket_object.zip_library.id}"
}

output "bucket_arn" {
  value = aws_s3_bucket.script_bucket.arn
}