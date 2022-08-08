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
  default = "MySql Community"
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

