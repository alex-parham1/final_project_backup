resource "aws_kinesis_firehose_delivery_stream" "s3_stream" {
  name        = "team-yogurt-tf-stream"
  destination = "extended_s3"
  kinesis_source_configuration {
    kinesis_stream_arn = var.kinesis_stream_arn_team_3
    role_arn           = var.role_arn_yogurt
  }

  extended_s3_configuration {
    role_arn   = var.role_arn_yogurt
    bucket_arn = "arn:aws:s3:::team-3-data-stream"

    processing_configuration {
      enabled = "true"

      processors {
        type = "Lambda"

        parameters {
          parameter_name  = "LambdaArn"
          parameter_value = "${aws_lambda_function.lambda_processor.arn}:$LATEST"
        }
      }
    }
    prefix = "cleaned"

    error_output_prefix = "error"
  }
}


resource "aws_lambda_function" "lambda_processor" {
  function_name = "team-yogurt-firehose-lambda"
  role          = var.lambda_role
  timeout       = 120
  package_type  = "Image"
  image_uri     = var.image_uri_ecr
  image_config { command = ["firehose_lambda.lambda_handler"] }
  environment {
    variables = {
      "debug" = var.debug
    }
  }
}


