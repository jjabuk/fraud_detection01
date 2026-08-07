// Dataset scope by default: most tables are created by application
// code/pipelines. ieee_train_transaction_raw is the deliberate exception
// -- its schema is pinned (src/fraud_detection/schemas/
// train_transaction_bq_schema.json, generated + audited via the
// raw_transaction_bq_schema Dagster asset) and worth managing as real
// infrastructure, so a schema change shows up as a reviewable `tofu plan`
// diff instead of silently re-inferring on the next pipeline run.

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

resource "google_bigquery_table" "ieee_train_transaction_raw" {
  dataset_id  = google_bigquery_dataset.raw.dataset_id
  table_id    = "ieee_train_transaction_raw"
  description = "Full IEEE-CIS train_transaction.csv dump, WRITE_TRUNCATE-loaded by raw_transactions_bigquery (Dagster)."
  labels      = var.labels

  schema = file("${path.module}/../schemas/train_transaction_bq_schema.json")

  # Deliberately NOT ignore_changes on schema: the whole point of managing
  # this table here is that a schema edit shows up as a plan diff. If a
  # future change is BigQuery-incompatible in place (e.g. a type change,
  # not just adding a nullable field), apply surfaces that as an API
  # error -- not a silent recreate.
  deletion_protection = true

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
