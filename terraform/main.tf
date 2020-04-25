# Using a single workspace:
terraform {
  backend "remote" {
    hostname     = "app.terraform.io"
    organization = "deo"

    workspaces {
      prefix = "prodstats-" # workspace qualifier
    }
  }
}

provider "aws" {
  region  = "us-east-1"
  profile = replace(var.environment, "stage", "dev") # remaps stage to use dev network
}

locals {
  full_service_name = "${var.service_name}-${var.environment}"

  tags = {
    environment  = var.environment
    terraform    = true
    domain       = var.domain
    service_name = var.service_name
  }
}

# Load VPC Data Source
data "terraform_remote_state" "vpc" {
  backend = "remote"

  config = {
    organization = "deo"
    workspaces = {
      name = "networking-${replace(var.environment, "stage", "dev")}" # remaps stage to use dev network
    }
  }
}


# Load shared ecs cluster
data "terraform_remote_state" "ecs_cluster" {
  backend = "remote"

  config = {
    organization = "deo"
    workspaces = {
      name = "ecs-collector-cluster-${replace(var.environment, "stage", "dev")}" # remaps stage to use dev network
    }
  }
}

data "terraform_remote_state" "web_cluster" {
  backend = "remote"

  config = {
    organization = "deo"
    workspaces = {
      name = "ecs-web-cluster-${replace(var.environment, "stage", "dev")}" # remaps stage to use dev network
    }
  }
}

data "terraform_remote_state" "kms" {
  backend = "remote"

  config = {
    organization = "deo"
    workspaces = {
      name = "kms-${replace(var.environment, "stage", "dev")}" # remaps stage to use dev network
    }
  }
}

# Get current account id that terraform is running under
data "aws_caller_identity" "current" {
}
