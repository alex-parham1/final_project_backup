data "aws_ami" "amazon_linux" {
  most_recent = true
  filter {
    name   = "name"
    values = ["amzn2-ami-kernel-5.10-hvm-2.0.*-x86_64-gp2"]
  }
  owners = [ "amazon" ]
}

resource "aws_iam_instance_profile" "grafana_iam" {
  name = "team_yogurt_grafana_tf"
  role = "ssm-ec2-role"
}

resource "aws_instance" "grafana_ec2" {
  ami                  = data.aws_ami.amazon_linux.id
  instance_type        = var.instance_type
  iam_instance_profile = aws_iam_instance_profile.grafana_iam.name
  subnet_id            = var.public_subnet
  security_groups      = ["sg-0ef2c1a8a7458f4aa"]
  tags = merge(
    tomap({"Name" = "team-yogurt-grafana-instance-tf"})
  )
}