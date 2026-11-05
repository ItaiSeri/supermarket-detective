terraform {
  required_version = ">= 1.6"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 6.0"
    }
  }
}

provider "aws" {
  region  = "il-central-1"
  profile = "default"

  default_tags {
    tags = {
      Project   = "supermarket-detective"
      ManagedBy = "terraform"
    }
  }
}