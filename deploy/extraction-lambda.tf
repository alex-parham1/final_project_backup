data "aws_s3_bucket" "data_bucket" {
  bucket = "team-3-data"
}


resource "aws_lambda_function" "terraform_lambda_func" {
  function_name = "team-yogurt-extraction-tf"
  role          = "arn:aws:iam::156058766667:role/yogurt-role"
  package_type = "Image"
  image_uri = "156058766667.dkr.ecr.eu-west-1.amazonaws.com/team_yogurt:latest"
  timeout = 300
  }


resource "aws_s3_bucket_notification" "test_bucket" {
    bucket = data.aws_s3_bucket.data_bucket.bucket
    lambda_function {
      lambda_function_arn = aws_lambda_function.terraform_lambda_func.arn
      events = ["s3:ObjectCreated:*"]
    }
    depends_on = [aws_lambda_function.terraform_lambda_func]
  }


resource "aws_lambda_permission" "s3_permission" {
  statement_id = "AllowExecutionFromS3Bucket"
  action = "lambda:InvokeFunction"
  function_name = aws_lambda_function.terraform_lambda_func.function_name
  principal = "s3.amazonaws.com"
}

