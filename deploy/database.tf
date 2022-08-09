resource "aws_db_instance" "db_yogurt" {
  name                    = "yogtf"
  identifier              = "thirstee-grafana-db"
  engine                  = var.engine
  allocated_storage       = 20
  instance_class          = var.instance_class
  db_subnet_group_name    = aws_db_subnet_group.main.name
  vpc_security_group_ids  = [aws_security_group.rds.id]
  username                = var.db_username
  password                = var.db_password
  backup_retention_period = 0
  port                    = 3306
  skip_final_snapshot     = true
  multi_az                = false
}

resource "aws_security_group" "rds" {
  description = "Allow access to the RDS database instance"
  name        = "yogurt-rds-inbound-access"
  # AcademySharedInfraStack/SainsburysSharedVpc
  vpc_id = "vpc-0e296a5c8aac14d8c"

  ingress {
    protocol  = "tcp"
    from_port = 3306
    to_port   = 3306

    security_groups = [aws_security_group.grafana.id
    ]
  }
}

resource "aws_db_subnet_group" "main" {
  name = "yogurt-main"
  subnet_ids = [
    # AcademySharedInfraStack/SainsburysSharedVpc/privateSubnet1
    "subnet-03f1b73d915b12b0d",
    # AcademySharedInfraStack/SainsburysSharedVpc/privateSubnet2
    "subnet-02d1e63f3fc1ada51",
  ]

  tags = merge(
    tomap({ "Name" = "yogurt-main" })
  )
}

