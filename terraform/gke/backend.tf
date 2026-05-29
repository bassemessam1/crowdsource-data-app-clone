terraform {
  backend "gcs" {
    bucket = "crowdsource-app-tfstate-crowdsource-data-app-clone"
    prefix = "gke"
  }
}