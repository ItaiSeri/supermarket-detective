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

# Ohio (us-east-2): Databricks runs there, so the Kaggle raw files and the Kaggle download lane live there too
provider "aws" {
  alias   = "ohio"
  region  = "us-east-2"
  profile = "default"

  default_tags {
    tags = {
      Project   = "supermarket-detective"
      ManagedBy = "terraform"
    }
  }
}
