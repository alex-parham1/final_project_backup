data "aws_s3_bucket" "clean_data_bucket" {
  bucket = "team-yogurt-cleaned-data"
}


resource "aws_lambda_function" "db_terraform_lambda_func" {
  function_name = "team-yogurt-db-tf"

  role = "arn:aws:iam::156058766667:role/yogurt-role"

  package_type = "Image"

  image_uri = "156058766667.dkr.ecr.eu-west-1.amazonaws.com/team_yogurt:latest"

  timeout = 900

  memory_size = 600

  reserved_concurrent_executions = 2


  dead_letter_config {
    target_arn = "arn:aws:sns:eu-west-1:156058766667:data_team_3"
  }

  environment {
    variables = {
      SNOWFLAKE_PASS = var.SNOWFLAKE_PASS
      SNOWFLAKE_USER = var.SNOWFLAKE_USER
      mysql_db       = var.mysql_db
      mysql_host     = var.mysql_host
      mysql_pass     = var.mysql_pass
      mysql_port     = var.mysql_port
      mysql_user     = var.mysql_user
      debug          = var.debug
    }
  }
  vpc_config {
    subnet_ids         = ["subnet-03f1b73d915b12b0d"]
    security_group_ids = ["sg-0a5924c5c0d69b627"]
  }
}

resource "aws_s3_bucket_notification" "test_bucket_tf" {
  bucket = data.aws_s3_bucket.clean_data_bucket.bucket
  lambda_function {
    lambda_function_arn = aws_lambda_function.db_terraform_lambda_func.arn
    events              = ["s3:ObjectCreated:*"]
  }
  depends_on = [aws_lambda_function.db_terraform_lambda_func]
}

resource "aws_lambda_permission" "s3_permission_tf" {
  statement_id  = "AllowExecutionFromS3Bucket"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.db_terraform_lambda_func.function_name
  principal     = "s3.amazonaws.com"
}

resource "aws_lambda_function_event_invoke_config" "timeout_setting" {
  function_name          = aws_lambda_function.terraform_lambda_func.function_name
  maximum_retry_attempts = 0
}
