// Dataset scope only. Tables are created by application code/pipelines.

resource "google_bigquery_dataset" "raw" {
  dataset_id  = "raw"
  location    = var.bq_location
  description = "Raw IEEE-CIS ingest data loaded from CSV files"
  labels      = var.labels

  delete_contents_on_destroy = false

  lifecycle {
    prevent_destroy = true
  }
}

resource "google_bigquery_dataset" "features" {
  dataset_id  = "features"
  location    = var.bq_location
  description = "Feature store for point-in-time engineered fraud features"
  labels      = var.labels

  delete_contents_on_destroy = false

  lifecycle {
    prevent_destroy = true
  }
}

resource "google_bigquery_dataset" "prediction_logs" {
  dataset_id  = "prediction_logs"
  location    = var.bq_location
  description = "Inference request/prediction logs for monitoring and drift analysis"
  labels      = var.labels

  delete_contents_on_destroy = false

  lifecycle {
    prevent_destroy = true
  }
}
