terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "4.51.0"
    }
  }
}

provider "google" {
  credentials = file("gcp-de-zoomcamp-mohit-76a064c9e528.json")

  project = "gcp-de-zoomcamp-mohit"
  region  = "us-central1"
  zone    = "us-central1-c"
}

resource "google_storage_bucket" "ny_taxi_mohit" {
  name     = "ny_taxi_mohit"
  location = "US"
}
