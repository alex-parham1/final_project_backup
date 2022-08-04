terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 3.27"
    }
  }
  backend "s3" {
    bucket         = "data-team-3-terraform-state"
    key            = "data-team-3-terraform-state.tfstate"
    region         = "eu-west-1"
    encrypt        = true
    dynamodb_table = "data-team-3-terraform-state-lock"
  }

  required_version = ">= 1.1.0"
}

provider "aws" {
  profile = "bootcamp-sandbox"
  region  = "eu-west-1"
}

