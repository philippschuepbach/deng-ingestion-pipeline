terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "5.6.0"
    }
  }
}

provider "google" {
  credentials = file("../keys/my-greds.json")
  project     = "gdelt-live-ingestion-489312"
  region      = "eu"
}

resource "google_storage_bucket" "demo-bucket" {
  name          = "gdelt-pipeline"
  location      = "eu"
  force_destroy = true


  lifecycle_rule {
    condition {
      age = 1
    }
    action {
      type = "AbortIncompleteMultipartUpload"
    }
  }
}



resource "google_bigquery_dataset" "demo_dataset" {
  dataset_id = "gdelt_pipeline_dataset"
  location   = "eu"
}