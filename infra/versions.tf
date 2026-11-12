terraform {
  required_version = ">= 1.6"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 6.0"
    }
    # zips the fetch_kaggle Lambda code (one .py file, no packages)
    archive = {
      source  = "hashicorp/archive"
      version = "~> 2.0"
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