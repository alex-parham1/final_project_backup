resource "aws_db_instance" "db_yogurt" {
    name = "team_yogurt-grafana-db"
    identifier = "thirstee-grafana-db"
    engine = var.engine
    allocated_storage = 20
    instance_class = var.instance_class
    db_subnet_group_name = aws_db_subnet_group.subnets.name
    vpc_security_group_ids = ["vpc-0e296a5c8aac14d8c"]
    username = var.db_username
    password = var.db_password
    backup_retention_period = 0
    port = 3305
    skip_final_snapshot = true
}

resource "aws_db_subnet_group" "subnets" {
    subnet_ids = [ var.subnets]
}