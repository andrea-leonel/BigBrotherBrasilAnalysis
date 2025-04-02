terraform {
  required_providers {
    google = {
      source = "hashicorp/google"
      version = "6.24.0"
    }
  }
}

provider "google" {
    project     = var.project
    region      = var.region
    credentials = file(var.credentials)
}

resource "google_storage_bucket" "bbb-bucket" {
  name          = var.gcs_bucket_name
  location      = var.location
  force_destroy = true

  lifecycle_rule {
    action {
      type = "AbortIncompleteMultipartUpload"
    }
    condition {
      age = 1
    }
  }
}
