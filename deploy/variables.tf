variable "SNOWFLAKE_USER" {
  description = "snowflake user name"
}

variable "SNOWFLAKE_PASS" {
  description = "snowflake user password"
}

variable "mysql_db" {
  description = "name of the database to access "
}

variable "mysql_host" {
  description = "host of db"
}

variable "mysql_pass" {
  description = "password for db user"
}

variable "mysql_port" {
  description = "port number for db"

}
variable "mysql_user" {
  description = "user name for db"

}

variable "db_password" {
  description = "password for the tf db"
}

variable "db_username" {
  description = "user name for the tf db"
}

variable "instance_class" {
  default = "db.t3.micro"
}

variable "engine" {
  default = "mysql"
}

variable "subnets" {
  description = "subnets to use"
}

variable "instance_type" {
  default = "t2.micro"
}

variable "public_subnet" {
  description = "a public subnet to use for grafana ec2"
}

variable "debug" {
  default = "False"
}

variable "lambda_role" {
  default = "arn:aws:iam::156058766667:role/yogurt-role"
}

variable "image_uri_ecr" {
  default = "156058766667.dkr.ecr.eu-west-1.amazonaws.com/team_yogurt:latest"
}

variable "role_arn_yogurt" {
  default = "arn:aws:iam::156058766667:role/team-yogurt-firehose-role"
}

variable "kinesis_stream_arn_team_3" {
  default = "arn:aws:kinesis:eu-west-1:156058766667:stream/team-3-data"
}