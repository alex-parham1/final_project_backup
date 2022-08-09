resource "aws_kinesis_firehose_delivery_stream" "s3_stream" {
  name        = "team-yogurt-tf-stream"
  destination = "extended_s3"
  kinesis_source_configuration {
    kinesis_stream_arn = "arn:aws:kinesis:eu-west-1:156058766667:stream/team-3-data"
    role_arn           = "arn:aws:iam::156058766667:role/team-yogurt-firehose-role"
  }

  extended_s3_configuration {
    role_arn   = "arn:aws:iam::156058766667:role/team-yogurt-firehose-role"
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
  role          = "arn:aws:iam::156058766667:role/yogurt-role"
  timeout       = 120
  package_type  = "Image"
  image_uri     = "156058766667.dkr.ecr.eu-west-1.amazonaws.com/team_yogurt:latest"
  image_config { command = ["firehose_lambda.lambda_handler"] }
}