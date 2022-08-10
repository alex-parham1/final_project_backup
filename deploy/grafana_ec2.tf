data "aws_ami" "amazon_linux" {
  most_recent = true
  filter {
    name   = "name"
    values = ["amzn2-ami-kernel-5.10-hvm-2.0.*-x86_64-gp2"]
  }
  owners = ["amazon"]
}

resource "aws_iam_instance_profile" "grafana_iam" {
  name = "team_yogurt_grafana_tf"
  role = "ec2-grafana-role"
}

resource "aws_instance" "grafana_ec2" {
  ami                    = data.aws_ami.amazon_linux.id
  instance_type          = var.instance_type
  iam_instance_profile   = aws_iam_instance_profile.grafana_iam.name
  subnet_id              = var.public_subnet
  vpc_security_group_ids = [aws_security_group.grafana.id]
  user_data              = file("./templates/ec2/user-data.sh")
  tags = merge(
    tomap({ "Name" = "team-yogurt-grafana-instance-tf" })
  )
  lifecycle {
    # prevent_destroy = true
    ignore_changes = [user_data]
  }
}

resource "aws_security_group" "grafana" {
  name   = "yogurt-grafana-tf-db"
  vpc_id = "vpc-0e296a5c8aac14d8c"
  lifecycle {
    ignore_changes = [description]
  }
  ingress {
    protocol    = "tcp"
    from_port   = 443
    to_port     = 443
    cidr_blocks = ["0.0.0.0/0"]
  }

  ingress {
    protocol    = "tcp"
    from_port   = 80
    to_port     = 80
    cidr_blocks = ["0.0.0.0/0"]
  }

  egress {
    protocol    = "tcp"
    from_port   = 443
    to_port     = 443
    cidr_blocks = ["0.0.0.0/0"]
  }

  egress {
    protocol    = "tcp"
    from_port   = 80
    to_port     = 80
    cidr_blocks = ["0.0.0.0/0"]
  }

  egress {
    from_port = 3306
    to_port   = 3306
    protocol  = "tcp"
    cidr_blocks = [
      # AcademySharedInfraStack/SainsburysSharedVpc/privateSubnet1 CIDR
      "10.0.12.0/22",
      # AcademySharedInfraStack/SainsburysSharedVpc/privateSubnet2 CIDR
      "10.0.16.0/22",
    ]
    security_groups = ["sg-03abcffa9c7e963b4"]
  }
}